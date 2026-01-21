import pandas as pd

# --- Step 1: Load your CSV files ---
passing = pd.read_csv("data/raw/nfl_passing.csv")     # make sure file names match
rushing = pd.read_csv("data/raw/nfl_rushing.csv")
receiving = pd.read_csv("data/raw/nfl_receiving.csv")

print("PASSING COLS:", list(passing.columns))
print("RUSHING COLS:", list(rushing.columns))
print("RECEIVING COLS:", list(receiving.columns))

# Standardize key columns + make yards columns unique
passing   = passing.rename(columns={"Player":"player","Team":"team","Pos":"pos","Age":"age","Yds":"pass_yds"})
rushing   = rushing.rename(columns={"Player":"player","Team":"team","Pos":"pos","Age":"age","Yds":"rush_yds"})
receiving = receiving.rename(columns={"Player":"player","Team":"team","Pos":"pos","Age":"age","Yds":"rec_yds"})


# --- Step 2: Merge datasets on player name ---
# Assuming each dataset has a 'Player' column
data = passing.merge(rushing, on='player', how='outer', suffixes=('', "_rush")) \
              .merge(receiving, on='player', how='outer', suffixes=('', "_rec"))

# Resolve pos/team across merges (pick first non-null)
data["pos"] = data["pos"].fillna(data.get("pos_rush")).fillna(data.get("pos_rec"))
data["team"] = data["team"].fillna(data.get("team_rush")).fillna(data.get("team_rec"))
data["age"] = data["age"].fillna(data.get("age_rush")).fillna(data.get("age_rec"))


# --- Step 3: Clean missing values ---
data.fillna(0, inplace=True)

# --- Step 4: Calculate total yards (optional, adjust if you want) ---
data['Total_Yards'] = data['pass_yds'] + data['rush_yds'] + data['rec_yds']

# --- Step 5: ML-style labeling (percentile rank within position group) ---
def score_row(row):
    pos = str(row.get("pos", "")).upper()
    if pos == "QB":
        return row.get("pass_yds", 0)
    elif pos == "RB":
        return row.get("rush_yds", 0) + row.get("rec_yds", 0)
    else:
        return row.get("rec_yds",0)

data["score"] = data.apply(score_row, axis=1)

def label_group(g):
    g = g.copy()
    # best players should have pct_rank near 1.0

    g["pct_rank"] = g["score"].rank(pct=True, ascending=True)
    g["Season_Label"] = "Underperformed"
    g.loc[g["pct_rank"] >= 0.50, "Season_Label"] = "Good"
    g.loc[g["pct_rank"] >= 0.90, "Season_Label"] = "Elite"
    return g

data = data.groupby("pos", dropna=False, group_keys=False).apply(label_group)

# --- Step 6: Save final labeled CSV ---
data.to_csv("NFL_2025_Labeled_Stats.csv", index=False)

print("Pipeline complete! Labeled dataset saved as 'NFL_2025_Labeled_Stats.csv'.")
