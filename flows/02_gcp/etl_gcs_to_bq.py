from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


# @task(retries=3)
# def extract_from_gcs(color: str, year: int, month: int) -> Path:
#     """Download trip data from GCS"""
#     gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
#     gcs_block = GcsBucket.load("zoom-gcs")
#     gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
#     return Path(f"../data/{gcs_path}")


# @task()
# def transform(path: Path) -> pd.DataFrame:
#     """Data cleaning example"""
#     df = pd.read_parquet(path)
#     print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
#     df["passenger_count"].fillna(0, inplace=True)
#     print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
#     return df


# @task()
# def write_bq(df: pd.DataFrame) -> None:
#     """Write DataFrame to BiqQuery"""

#     gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

#     df.to_gbq(
#         destination_table="dezoomcamp.yellow_2019_feb",
#         project_id="organic-cursor-412900",
#         credentials=gcp_credentials_block.get_credentials_from_service_account(),
#         chunksize=500_000,
#         if_exists="append",
#     )


# @flow()
# def etl_gcs_to_bq():
#     """Main ETL flow to load data into Big Query"""
#     # yellow taxi data
#     month = 2
#     color = "yellow"
#     year = 2019
#     # green taxi data
#     # month = 1
#     # color = "green"
#     # year = 2020
#     path = extract_from_gcs(color, year, month)
#     df = transform(path)
#     write_bq(df)


# if __name__ == "__main__":
#     etl_gcs_to_bq()

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02d}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    local_path = Path(f"./data/{color}/{color}_tripdata_{year}-{month:02d}.parquet")
    local_path.parent.mkdir(parents=True, exist_ok=True)
    gcs_block.get(from_path=gcs_path, local_path=local_path)
    return local_path

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data loading without transformation"""
    df = pd.read_parquet(path)
    return df

@task()
def write_bq(df: pd.DataFrame, color: str, year: int, month: int) -> None:
    """Write DataFrame to BigQuery"""
    destination_table = f"dezoomcamp.{color}_{year}_{month:02d}"
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table=destination_table,
        project_id="organic-cursor-412900",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists="append",
    )

@flow(log_prints=True)
def etl_gcs_to_bq(months: list[int], year: int, color: str) -> None:
    """Main ETL flow to load data into Big Query"""
    for month in months:
        path = extract_from_gcs(color, year, month)
        df = transform(path)
        write_bq(df, color, year, month)
        print(f"Processed {len(df)} rows for {color} taxi data, {year}-{month:02d}")

if __name__ == "__main__":
    etl_gcs_to_bq([2, 3], 2019, "yellow")