# scripts/extract.py
import os
import pandas as pd
from sqlalchemy import create_engine

# --- Update these if needed ---
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "")          # set in env or leave blank if none
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "postgres")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "raw")

files_and_tables = [
    ("nfl_passing.csv", "raw_passing"),
    ("nfl_rushing.csv", "raw_rushing"),
    ("nfl_receiving.csv", "raw_receiving"),
]

for filename, table in files_and_tables:
    path = os.path.join(DATA_DIR, filename)
    df = pd.read_csv(path)

    # Normalize column names to match your Postgres schema (lowercase + underscores)
    df.columns = (
        df.columns.str.strip()
                 .str.lower()
                 .str.replace("%", "_pct", regex=False)
                 .str.replace("/", "_", regex=False)
                 .str.replace("-", "_", regex=False)
                 .str.replace(" ", "_", regex=False)
    )

    # Ensure reserved keyword column is correct: INT
    if "int" in df.columns:
        df.rename(columns={"int": "int"}, inplace=True)  # keep name; table uses "int"

    # Replace NaN with None so Postgres inserts NULL
    df = df.where(pd.notnull(df), None)

    # Load into Postgres (replace ensures reruns are clean while you’re building)
    df.to_sql(table, engine, if_exists="replace", index=False)
    print(f"Loaded {len(df):,} rows into {table}")

print("Done.")
