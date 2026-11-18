
from prefect import flow

from etl.extract import extract_data
from etl.validate import validate_data
from etl.transform import transform_data
from etl.load import load_into_postgres

@flow(
    name="Daily Police Log ETL Pipeline",
    log_prints=True
)
def daily_police_log_pipeline():

    print("Starting Daily Police Log ETL Pipeline")

    # Stage 1: Extract
    print("\n1. Extracting data...")
    extracted_df = extract_data()

    # Stage 2: Validate
    print("\n2. Validating data...")
    validated_df = validate_data(extracted_df)

    # Stage 3: Transform
    print("\n3. Transforming data...")
    transformed_df = transform_data(validated_df)

    # Stage 4: Load
    print("\n4. Loading data into PostgreSQL...")
    load_into_postgres(transformed_df)

    print("\nETL Pipeline completed successfully!")

    return transformed_df

if __name__ == "__main__":
    daily_police_log_pipeline()