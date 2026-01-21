import os
import pandas as pd
from sqlalchemy import create_engine

DB_USER = os.getenv("PGUSER", "chris")
DB_PASS = os.getenv("PGPASSWORD", "6812")
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "postgres")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Pull Feature Table
df = pd.read_sql("SELECT * FROM player_features_2025;", engine)

# Decide which metric to rank by per position group
def score_row(row):
    pos = (row["pos"] or "").upper()
    if pos == "QB":
        return row["pass_yds"] #QB Signal
    elif pos == "RB":
        return row["rush_yds"] + row["rec_yds"] # RB signal
    else:
        return row["rec_yds"] #WR/TE/Others

df["score"] = df.apply(score_row, axis=1)

# Label within each position group using percent ranks
def label_group(g):
    g = g.copy()
    # percent rank: 1.0 = best
    g["pct_rank"] = g["score"].rank(pct=True, ascending=True)
    # ascending=True means small scores get small pct; we want top scores => high pct
    # Elite: top 10% => pct_rank >= 0.90
    g["season_label"] = "Underperformed"
    g.loc[g["pct_rank"] >= 0.50, "season_label"] = "Good"
    g.loc[g["pct_rank"] >= 0.90, "season_label"] = "Elite"
    return g

df = df.groupby("pos", dropna=False, group_keys=False).apply(label_group)

# Write labels back to Postgres
out = df[
    ["player", "team", "pos", "age", "season_label", "score", "pct_rank"]
].copy()
out.to_sql("season_labels_2025", engine, if_exists="replace", index=False)

print("Wrote labels to Postgres table: season_labels_2025")

