
import pandas as pd
from pathlib import Path
from prefect import task


PROJECT_ROOT = Path(__file__).resolve().parent.parent

PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)


@task(name="Transform Daily Police Log Data")
def transform_data(df: pd.DataFrame) -> pd.DataFrame:

    print("Starting data transformation...")

    if df.empty:
        raise ValueError("Cannot transform an empty dataset.")

    df = df.copy()

    # 1. Remove duplicate records using the incident ID
    before = len(df)

    df = df.drop_duplicates(subset=["id"], keep="first")

    duplicates_removed = before - len(df)

    print(f"Duplicate records removed: {duplicates_removed}")

    # 2. Remove records with missing IDs
    df = df.dropna(subset=["id"])

    # 3. Convert date_time into datetime format
    df["date_time"] = pd.to_datetime(
        df["date_time"],
        errors="coerce",
        utc=True
    )

    # Remove records with invalid timestamps
    df = df.dropna(subset=["date_time"])

    # 4. Extract time-based features
    df["year"] = df["date_time"].dt.year
    df["month"] = df["date_time"].dt.month
    df["day"] = df["date_time"].dt.day
    df["hour"] = df["date_time"].dt.hour
    df["minute"] = df["date_time"].dt.minute
    df["second"] = df["date_time"].dt.second

    if df.empty:
        raise ValueError("No valid records remain after transformation.")

    # 5. Save processed data
    output_file = (
        PROCESSED_DATA_DIR / "daily_police_log_processed.csv"
    )

    df.to_csv(output_file, index=False)

    print(f"Records after transformation: {len(df)}")
    print(f"Processed data saved to: {output_file}")

    return df
