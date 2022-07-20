import os
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

from datetime import datetime
from google.cloud import storage
import pandas as pd
from glom import glom
import pyarrow.parquet as pq
import json
import gzip

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
DATASET = "de_project_711"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'de_project_711')

#### Modify below ####
url = 'https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv'
csv_file = 'epidemiology.csv'
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

schema = [
    {
        "name": "date",
        "type": "DATE"
    },
    {
        "name": "location_key",
        "type": "STRING"
    },
    {
        "name": "new_confirmed",
        "type": "INTEGER"
    },
    {
        "name": "new_deceased",
        "type": "INTEGER"
    },
    {
        "name": "new_recovered",
        "type": "INTEGER"
    },
    {
        "name": "new_tested",
        "type": "INTEGER"
    },
    {
        "name": "cumulative_confirmed",
        "type": "INTEGER"
    },
    {
        "name": "cumulative_deceased",
        "type": "INTEGER"
    },
    {
        "name": "cumulative_recovered",
        "type": "INTEGER"
    },
    {
        "name": "cumulative_tested",
        "type": "INTEGER"
    },
]

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

with DAG(
    dag_id="covid19_data_ingestion",
    schedule_interval="0 0 1 * *",
    start_date=datetime(2022, 7, 1),
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de-project'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f'curl -sSLf {url} > {path_to_local_home}/{csv_file}',
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{csv_file}",
            "local_file": f"{path_to_local_home}/{csv_file}",
        },
    )
    gcs_2_bq_ext_task = BigQueryCreateExternalTableOperator(
        task_id=f"bq_{DATASET}_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{DATASET}_external_table",
            },
            "schema": {
                "fields": schema
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": "CSV",
                "Ignore unknown values": "false",
                "Max bad records": 0,
                "sourceUris": [f"gs://{BUCKET}/raw/{csv_file}"],
            },
        },
    )

    remove_files_task = BashOperator(
        task_id="remove_files_task",
        bash_command=f"rm {path_to_local_home}/{csv_file}"
    )

    download_dataset_task >> local_to_gcs_task >> gcs_2_bq_ext_task  >> remove_files_task