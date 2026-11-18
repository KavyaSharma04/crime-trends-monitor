
import pandas as pd
from collections import Counter
from prefect import task


EXPECTED_COLUMNS = [
    "date_time",
    "id",
    "type",
    "subtype",
    "location",
    "last_updated",
    "description"
]

REQUIRED_COLUMNS = [
    "date_time",
    "id",
    "type",
    "location"
]


@task(name="Validate Daily Police Log Data")
def validate_data(df: pd.DataFrame) -> pd.DataFrame:

    if df.empty:
        raise ValueError("The extracted dataset is empty.")

    print("Starting data validation...")

    # 1. Validate schema
    if Counter(df.columns) != Counter(EXPECTED_COLUMNS):
        raise ValueError(
            f"Invalid schema. Expected: {EXPECTED_COLUMNS}, "
            f"Received: {list(df.columns)}"
        )

    print("Schema validation passed.")

    # 2. Validate numeric IDs
    numeric_ids = pd.to_numeric(df["id"], errors="coerce")

    if numeric_ids.isna().any():
        raise ValueError("Invalid or missing IDs found in dataset.")

    df = df.copy()
    df["id"] = numeric_ids

    print("ID validation passed.")

    # 3. Validate datetime values
    parsed_dates = pd.to_datetime(
        df["date_time"],
        errors="coerce",
        utc=True
    )

    if parsed_dates.isna().any():
        raise ValueError("Invalid date_time values found.")

    print("Datetime validation passed.")

    # 4. Check missing values in required columns
    missing_values = df[REQUIRED_COLUMNS].isnull().sum()

    if missing_values.any():
        raise ValueError(
            f"Missing values found:\n{missing_values}"
        )

    print("Missing-value validation passed.")
    print("All validation checks passed successfully!")

    return df
