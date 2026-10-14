# database.py
import duckdb
from contextlib import contextmanager

class DatabaseManager:
 def __init__(self, db_path="sales.db"):
 self.db_path = db_path
 
 def get_connection(self):
 """Get a new DuckDB connection (thread-safe)"""
 con = duckdb.connect(self.db_path)
 con.execute("SET memory_limit='2GB';")
 con.execute("SET threads=4;")
 return con
 
 @contextmanager
 def session(self):
 """Context manager, auto-closes connection"""
 con = self.get_connection()
 try:
 yield con
 finally:
 con.close()

db_manager = DatabaseManager()
