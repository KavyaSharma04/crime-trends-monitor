
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sodapy import Socrata
from prefect import task


# Project directories
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Cambridge Daily Police Log dataset
DOMAIN = "data.cambridgema.gov"
DATASET_ID = "3gki-wyrb"


@task(
    name="Extract Daily Police Log",
    retries=3,
    retry_delay_seconds=10
)
def extract_data():

    load_dotenv(PROJECT_ROOT / ".env")

    app_token = os.getenv("SOCRATA_APP_TOKEN")

    print("Connecting to Cambridge Open Data API...")

    client = Socrata(
        DOMAIN,
        app_token,
        timeout=60
    )

    try:
        results = client.get_all(DATASET_ID)

        df = pd.DataFrame.from_records(results)

        if df.empty:
            raise ValueError("API returned no records.")

        output_file = RAW_DATA_DIR / "daily_police_log_raw.csv"
        df.to_csv(output_file, index=False)

        print(f"Extracted records: {len(df)}")
        print(f"Columns received: {list(df.columns)}")
        print(f"Raw data saved to: {output_file}")

        return df

    finally:
        client.close()
