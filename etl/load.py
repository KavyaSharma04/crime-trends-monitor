
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from prefect import task

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_FILE = (
    PROJECT_ROOT / "data" / "processed" /
    "daily_police_log_processed.csv"
)

TABLE_NAME = "cpd_incidents"

@task(name="Load Daily Police Log into PostgreSQL")
def load_into_postgres(df: pd.DataFrame):

    load_dotenv(PROJECT_ROOT / ".env")

    if df.empty:
        raise ValueError("Cannot load an empty dataset.")

    required_env = ["POSTGRES_USER", "POSTGRES_PASSWORD"]

    for variable in required_env:
        if not os.getenv(variable):
            raise ValueError(f"Missing environment variable: {variable}")

    db_url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5435")),
        database=os.getenv("POSTGRES_DB", "cpd_db")
    )

    engine = create_engine(db_url, pool_pre_ping=True)

    try:
        print("Loading data into PostgreSQL...")

        # Ensure IDs are suitable for a primary key
        df = df.copy()
        df["id"] = pd.to_numeric(df["id"], errors="raise")

        if df["id"].isna().any() or df["id"].duplicated().any():
            raise ValueError("IDs must be present and unique.")

        if not (df["id"] % 1 == 0).all():
            raise ValueError("IDs must contain whole numbers.")

        df["id"] = df["id"].astype("int64")

        # Replace the previous snapshot with the latest processed data
        df.to_sql(
            name=TABLE_NAME,
            con=engine,
            schema="public",
            if_exists="replace",
            index=False,
            chunksize=500,
            method="multi"
        )

        # Add the primary key after loading
        with engine.begin() as connection:
            connection.execute(
                text(
                    f'ALTER TABLE public."{TABLE_NAME}" '
                    'ADD PRIMARY KEY ("id")'
                )
            )

            count = connection.execute(
                text(f'SELECT COUNT(*) FROM public."{TABLE_NAME}"')
            ).scalar_one()

        print(f"Processed records: {len(df)}")
        print(f"Records in PostgreSQL: {count}")

        if count != len(df):
            raise RuntimeError("Database record count does not match.")

        print("Data loaded and verified successfully!")

    finally:
        engine.dispose()