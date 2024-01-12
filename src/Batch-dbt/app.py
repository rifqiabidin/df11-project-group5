import os
import logging
import subprocess
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery, storage

from flask import Flask


def transform_timestamp(timestamp_str):
    # Adjust the format as per your timestamp format in the CSV
    return datetime.strptime(timestamp_str, '%d/%m/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')

def transform_date(date_str):
    return datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d')

app = Flask(__name__)

@app.route('/')
def hello():
    return "Hello!"

@app.route('/daily', methods=['POST'])
def daily():
    try:
        # Define project and location
        PROJECT_ID = "final-project-df11-group5"
        LOCATION = "asia-southeast2"

        # GCS 
        BUCKET_NAME = "datalake-bank"
        FOLDER_NAME = "marketing_bank"

        #BIGQUERY
        RAW_DATASET = "raw_final_project"
        RAW_TABLE = "raw_bank_marketing"
        GOLD_DATASET = "mart_final_project"
        GOLD_TABLE = "gold_bank_marketing"

        # Get yesterday's date in YYYY_MM_DD format
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_date = yesterday.date()  # Extract the date part
        yesterday_date_str = yesterday_date.strftime("%Y-%m-%d")

        # Construct GCS file path
        #file_path_testing = "marketing_bank/2020-11-11/combined_data_2020-11-11.csv"
        file_path = f"{FOLDER_NAME}/{yesterday_date_str}/combined_data_{yesterday_date_str}.csv"
        
        # Download the file from GCS 
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(file_path)
        blob.download_to_filename("raw.csv")
        
        # Transform the CSV file
        df = pd.read_csv("raw.csv", header= None)
        
        # Apply transformations to df 
        df[0] = df[0].apply(transform_timestamp)
        df[4] = df[4].apply(transform_date)
        df.to_csv("transformed_data.csv", header=None, index=None)

        # Load data into BigQuery raw table
        bigquery_client = bigquery.Client(project=PROJECT_ID)
        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV)
        with open("transformed_data.csv", "rb") as file_obj:
            load_job = bigquery_client.load_table_from_file(file_obj, f"{RAW_DATASET}.{RAW_TABLE}", job_config=job_config)
        
        load_job.result()  # Wait for the job to complete

        # Run dbt task
        project_dir = os.environ.get("DBT_PROJECT_DIR", "final_project")
        logging.info("Running dbt project in %s with profile %s", project_dir)
        try:
            subprocess.run(["dbt", "run", "--project-dir", "dbt", "--profiles-dir", 'dbt'], check=True)
            logging.info("dbt run completed successfully")
        except subprocess.CalledProcessError as e:
            logging.error("dbt run failed: %s", e)
            raise

        # Create the gold dataset if it doesn't exist
        dataset_ref = bigquery_client.dataset(GOLD_DATASET)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = LOCATION
        dataset = bigquery_client.create_dataset(dataset, exists_ok=True)

        # Create the gold table
        query = f"""
        CREATE OR REPLACE MATERIALIZED VIEW {PROJECT_ID}.{GOLD_DATASET}.{GOLD_TABLE} AS (
            SELECT
                f.id,
                f.join_yearmonth,
                da.age_group,
                dj.job_type,
                de.education_type,
                dm.marital_type,
                f.has_housing,
                f.has_loan,
                f.has_default,
            FROM
                {PROJECT_ID}.dwh_final_project.partclust_fact_bank_marketing f
            JOIN {PROJECT_ID}.dwh_final_project.dim_age_group da on f.age_group_id = da.age_group_id
            JOIN {PROJECT_ID}.dwh_final_project.dim_job dj on f.job_id = dj.job_id
            JOIN {PROJECT_ID}.dwh_final_project.dim_education de on f.education_id = de.education_id
            JOIN {PROJECT_ID}.dwh_final_project.dim_marital dm on f.marital_id = dm.marital_id
        );
        """
        bigquery_client.query(query).result()
        return "Data processing completed successfully", 200
    except Exception as e:
        logging.exception(e)
        return e

if __name__ == '__main__':
    server_port = os.environ.get('PORT', '8080')
    app.run(debug=False, port=server_port, host='0.0.0.0')