import os
import sys
import threading
import time
from pathlib import Path

import duckdb
import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")


def require(name: str) -> str:
    """Return a required setting from the environment or exit with an error."""
    value = os.getenv(name)
    if not value:
        sys.exit(f"Missing setting '{name}' in .env")
    return value


ACCOUNT_IDS = [
    int(line.split("#", 1)[0].strip())
    for line in require("ACCOUNT_IDS").replace(",", "\n").splitlines()
    if line.split("#", 1)[0].strip()
]
DB_PATH = BASE_DIR / os.getenv("DB_PATH", "data/history.duckdb")
MANIFEST_URL = os.getenv(
    "MANIFEST_URL", "https://data.deadlock-api.com/v1/manifest.json"
)
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "60"))
COLUMNS = ", ".join(c.strip() for c in require("COLUMNS").split(",") if c.strip())


def fmt(seconds: float) -> str:
    """Format seconds as mm:ss or h:mm:ss."""
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    """Run a query that returns a single integer value."""
    row = con.execute(sql).fetchone()
    assert row is not None
    return int(row[0])


class Spinner:
    """Shows the live elapsed time while a blocking query is running."""

    def __init__(self, prefix: str):
        self.prefix = prefix
        self.start = time.time()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        frames = "|/-\\"
        i = 0
        while not self._stop.is_set():
            sys.stdout.write(
                f"\r{self.prefix} {frames[i % 4]} {fmt(time.time() - self.start)}   "
            )
            sys.stdout.flush()
            i += 1
            time.sleep(0.2)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()
        # Clear the spinner line
        sys.stdout.write("\r" + " " * 100 + "\r")
        sys.stdout.flush()


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Fetch the manifest and collect the current-generation match_player files
    manifest = requests.get(MANIFEST_URL, timeout=REQUEST_TIMEOUT).json()
    base = manifest["public_url"].rstrip("/")
    table = manifest["tables"]["match_player"]
    gen = table.get("generation", 0)
    keys = sorted(f["key"] for f in table["files"] if f.get("generation", 0) == gen)
    ids = ", ".join(str(a) for a in ACCOUNT_IDS)

    con = duckdb.connect(str(DB_PATH))
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # Registry of finished files (also records files with zero matches)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_files (
            source_key VARCHAR PRIMARY KEY,
            row_count BIGINT,
            seconds DOUBLE,
            finished_at TIMESTAMP DEFAULT current_timestamp
        )
        """
    )
    done = {
        r[0] for r in con.execute("SELECT source_key FROM processed_files").fetchall()
    }
    todo = [k for k in keys if k not in done]

    print(f"Database: {DB_PATH}")
    print(f"Account IDs: {ACCOUNT_IDS}")
    print(f"Files total: {len(keys)} | already done: {len(done)} | remaining: {len(todo)}\n")

    has_matches = bool(
        scalar(
            con,
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'matches'",
        )
    )
    total_rows = scalar(con, "SELECT count(*) FROM matches") if has_matches else 0
    t0 = time.time()
    durations: list[float] = []

    try:
        for i, key in enumerate(todo, 1):
            short = key.split("match_player/")[-1]
            # Rough ETA based on the average duration so far
            eta = (
                f" | ETA ~{fmt(sum(durations) / len(durations) * (len(todo) - i + 1))}"
                if durations
                else ""
            )
            prefix = f"[{i}/{len(todo)}] {short} | Hits: {total_rows}{eta} |"
            query = f"""
                SELECT {COLUMNS}
                FROM read_parquet('{base}/{key}')
                WHERE account_id IN ({ids})
            """
            start = time.time()
            with Spinner(prefix):
                # One transaction per file: data and registry entry stay consistent
                con.execute("BEGIN")
                if not has_matches:
                    con.execute(f"CREATE TABLE matches AS {query}")
                    has_matches = True
                    n = scalar(con, "SELECT count(*) FROM matches")
                else:
                    n = scalar(con, f"SELECT count(*) FROM ({query})")
                    if n:
                        con.execute(f"INSERT INTO matches BY NAME {query}")
                con.execute(
                    "INSERT INTO processed_files (source_key, row_count, seconds) VALUES (?, ?, ?)",
                    [key, n, time.time() - start],
                )
                con.execute("COMMIT")
            took = time.time() - start
            durations.append(took)
            total_rows += n
            print(
                f"[{i}/{len(todo)}] OK  {short}  +{n} rows  ({fmt(took)})  total: {total_rows}"
            )
    except KeyboardInterrupt:
        try:
            con.execute("ROLLBACK")
        except duckdb.Error:
            pass
        print("\nAborted. The next run resumes with the unfinished file.")
        sys.exit(130)

    print(f"\nDone in {fmt(time.time() - t0)}. Rows in 'matches': {total_rows}")
    if has_matches:
        # Base, delta and residual files may overlap, so check for duplicates
        dups = scalar(
            con,
            """
            SELECT count(*) FROM (
                SELECT match_id, account_id FROM matches
                GROUP BY 1, 2 HAVING count(*) > 1
            )
            """,
        )
        print(f"Duplicate (match_id, account_id) pairs: {dups}")
    con.close()


if __name__ == "__main__":
    main()
