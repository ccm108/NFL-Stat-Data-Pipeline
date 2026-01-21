import pandas as pd

# --- Step 1: Load your CSV files ---
passing = pd.read_csv("data/nfl_passing.csv")     # make sure file names match
rushing = pd.read_csv("data/nfl_rushing.csv")
receiving = pd.read_csv("data/nfl_receiving.csv")

# --- Step 2: Merge datasets on player name ---
# Assuming each dataset has a 'Player' column
data = passing.merge(rushing, on='Player', how='outer') \
              .merge(receiving, on='Player', how='outer')

# --- Step 3: Clean missing values ---
data.fillna(0, inplace=True)

# --- Step 4: Calculate total yards (optional, adjust if you want) ---
data['Total_Yards'] = data.get('Passing_Yards', 0) + data.get('Rushing_Yards', 0) + data.get('Receiving_Yards', 0)

# --- Step 5: Label seasons based on Total_Yards percentile ---
def label_season(row, percentiles):
    if row['Total_Yards'] >= percentiles['Elite']:
        return 'Elite'
    elif row['Total_Yards'] >= percentiles['Above_Avg']:
        return 'Above Average'
    elif row['Total_Yards'] >= percentiles['Good']:
        return 'Good'
    else:
        return 'Bad'

# Calculate percentiles
percentiles = {
    'Elite': data['Total_Yards'].quantile(0.9),
    'Above_Avg': data['Total_Yards'].quantile(0.7),
    'Good': data['Total_Yards'].quantile(0.4)
}

# Apply labeling
data['Season_Label'] = data.apply(lambda row: label_season(row, percentiles), axis=1)

# --- Step 6: Save final labeled CSV ---
data.to_csv("NFL_2025_Labeled_Stats.csv", index=False)

print("Pipeline complete! Labeled dataset saved as 'NFL_2025_Labeled_Stats.csv'.")
