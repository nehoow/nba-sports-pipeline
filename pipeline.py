import pandas as pd
import sqlite3
from datetime import datetime

#==================================
#           CONFIGURATION
#==================================
CONFIG = {
    "INPUT_FILE": "all_seasons.csv",
    "OUTPUT_FILE": "nba_clean.csv",
    "DB_FILE": "nba.db",
    "TABLE_NAME": "players",
    "min_seasons": 1,
    "elite_pits_threshold": 25
}
LOG_FILE = "pipeline_log.txt"
#==================================
#           LOGGER SETUP
#==================================
def log(message):
    timestamp = datetime.now().strftime("%m-%d-%Y %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    print(full_message)
    with open(LOG_FILE, "a") as log_file:
        log_file.write(full_message + "\n")

#==================================
#          EXTRACT FUNCTION
#==================================
def extract(filepath):
    log("Extracting data...")
    try:
        df = pd.read_csv(filepath)
        log(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns.")
        return df
    except FileNotFoundError:
        log(f"Error: File '{filepath}' not found. Check your folder.")
        raise

#==================================
#          TRANSFORM FUNCTION
#==================================
def transform(df):
    log("Transforming data...")

    # Remove missing values
    before = df.shape[0]
    df = df.dropna(subset=["player_name", "pts", "reb", "ast"])
    after = df.shape[0]
    log(f"Dropped {before - after} rows with missing values.")

    # Remove duplicates
    df = df.drop_duplicates()
    log(f"Removed duplicates - {df.shape[0]} rows remaining.")

    # Clean up column types
    df["pts"] = pd.to_numeric(df["pts"], errors="coerce")
    df["reb"] = pd.to_numeric(df["reb"], errors="coerce")
    df["ast"] = pd.to_numeric(df["ast"], errors="coerce")

    # Add a new column for total contributions
    df["total_contributions"] = (df["pts"] + df["reb"] + df["ast"]).round(2)

    # Add a performance category based on total contributions
    df["tier"] = pd.cut(
        df["pts"],
        bins=[0, 10, 20, 30, 100],
        labels=["role player", "starter", "star", "superstar"]
    )

    log("Added 'total contributions' and 'tier' columns.")
    log(f"Transformation complete - {df.shape[0]} clean rows ready")
    return df

#==================================
#       LOAD FUNCTION
#==================================
def load(df):
    log("Loading data...")

    # Save to csv
    df.to_csv(CONFIG["OUTPUT_FILE"], index=False)
    log(f"Saved clean data to {CONFIG['OUTPUT_FILE']}.")

    # Save to SQLite database
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    df.to_sql(CONFIG["TABLE_NAME"], conn, if_exists="replace", index=False)
    log(f"Saved to database '{CONFIG['DB_FILE']}' in table '{CONFIG['TABLE_NAME']}'.")

#==================================
#           REPORT
#==================================
def report(df):
    log("Generating report...")
    lines = []
    lines.append("=" * 40)
    lines.append("NBA Data Pipeline Report")
    lines.append(f"Generated: {datetime.now().strftime('%m-%d-%Y %H:%M:%S')}")
    lines.append("=" * 40)
    lines.append(f"Total records processed: {df.shape[0]}")
    lines.append(f"Season covered: {df["season"].nunique()}")
    lines.append(f"Total Player: {df["player_name"].nunique()}")
    lines.append("\n Top 5 Scorers")
    top = df.groupby(df["player_name"])["pts"].mean().sort_values(ascending=False).head(5)
    for player_name, pts in top.items():
        lines.append(f" {player_name:<25} {round(pts, 2)}")
    lines.append("\n Players by Tier")
    for tier, count in df["tier"].value_counts().items():
        lines.append(f"{str(tier):<15} {count}")
    lines.append("=" * 40)

    report_text = "\n".join(lines)
    print(report_text)
    with open("report.txt", "w") as f:
        f.write(report_text)
#==================================
#           MAIN FUNCTION
#==================================
if __name__ == "__main__":
    log("=========== NBA Data Pipeline Started ===========")
    raw_df = extract(CONFIG["INPUT_FILE"])
    clean_df = transform(raw_df)
    load(clean_df)
    report(clean_df)
    log("=========== NBA Data Pipeline Started ===========")