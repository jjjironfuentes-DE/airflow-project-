
import json
import pandas as pd
from pathlib import Path

# Paths to the input and output files
INPUT_FILE = Path("data/processed/top_5_tv_shows.json")
OUTPUT_FILE = Path("data/processed/top_5_tv_shows.csv")

# Load JSON data
with open(INPUT_FILE, "r", encoding="utf-8") as file:
    data = json.load(file)

# Create a DataFrame
df = pd.DataFrame(data)

# Select and organize columns
df = df[["rank", "title", "year", "imdb_id", "url"]]

# Display the results
print("\nTOP 5 IMDb TV Shows")
print(df.to_string(index=False))

# Export to CSV
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

print(f"\nCSV saved to: {OUTPUT_FILE}")
