import sys
import threading
import time
from pathlib import Path

import duckdb
import requests

ACCOUNT_IDS = [235303166, 57462604]  # HIER deine echten account_ids (Steam ID3) eintragen!
DB_PATH = Path("data") / "history.duckdb"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
COLUMNS = """
    match_id, start_time, duration_s, match_mode, account_id, team, hero_id,
    kills, deaths, assists, net_worth, last_hits, denies, player_level, won
"""


def fmt(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    assert row is not None
    return int(row[0])


class Spinner:
    """Zeigt während einer blockierenden Abfrage live die Laufzeit an."""

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
        sys.stdout.write("\r" + " " * 100 + "\r")
        sys.stdout.flush()


def main() -> None:
    manifest = requests.get(
        "https://data.deadlock-api.com/v1/manifest.json", timeout=60
    ).json()
    base = manifest["public_url"].rstrip("/")
    table = manifest["tables"]["match_player"]
    gen = table.get("generation", 0)
    keys = sorted(
        f["key"] for f in table["files"] if f.get("generation", 0) == gen
    )
    ids = ", ".join(str(int(a)) for a in ACCOUNT_IDS)

    con = duckdb.connect(DB_PATH)
    con.execute("INSTALL httpfs; LOAD httpfs;")
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

    print(f"Account-IDs: {ACCOUNT_IDS}")
    print(
        f"Dateien gesamt: {len(keys)} | bereits erledigt: {len(done)} | offen: {len(todo)}\n"
    )

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
            eta = (
                f" | Rest ca. {fmt(sum(durations) / len(durations) * (len(todo) - i + 1))}"
                if durations
                else ""
            )
            prefix = f"[{i}/{len(todo)}] {short} | Treffer: {total_rows}{eta} |"
            query = f"""
                SELECT {COLUMNS}
                FROM read_parquet('{base}/{key}')
                WHERE account_id IN ({ids})
            """
            start = time.time()
            with Spinner(prefix):
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
                f"[{i}/{len(todo)}] OK  {short}  +{n} Zeilen  ({fmt(took)})  gesamt: {total_rows}"
            )
    except KeyboardInterrupt:
        try:
            con.execute("ROLLBACK")
        except duckdb.Error:
            pass
        print("\nAbgebrochen. Beim nächsten Start geht es bei der offenen Datei weiter.")
        sys.exit(130)

    print(f"\nFertig in {fmt(time.time() - t0)}. Zeilen in 'matches': {total_rows}")
    if has_matches:
        dups = scalar(
            con,
            """
            SELECT count(*) FROM (
                SELECT match_id, account_id FROM matches
                GROUP BY 1, 2 HAVING count(*) > 1
            )
            """,
        )
        print(f"Doppelte (match_id, account_id)-Paare: {dups}")
    con.close()


if __name__ == "__main__":
    main()
