from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
import boto3
import json
import snowflake.connector

snowflake_connection = 'snowflake_SVC_DATA_SCIENCE'
bucket_name = 'gen-dope-lab-mike-snowflake-test'
file_name = 'snowflake_data.parquet'
snowflake_query = """
  SELECT
    *
  FROM
    ALINEAN.ROI_BENEFIT_TRANSFORMATION
  LIMIT 10
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'dagrun_timeout': timedelta(hours=2),
    'execution_timeout': timedelta(hours=1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Snowflake_to_S3',
    default_args=default_args,
    description='Example DAG demonstrating Snowflake and S3 connections',
    is_paused_upon_creation=False,
    schedule_interval=None,
)

def pull_from_snowflake(ds, **kwargs):
    # Get creds from Airflow:
    connection = BaseHook.get_connection(snowflake_connection)
    snowflake_user = connection.login
    snowflake_password = connection.get_password()
    snowflake_account = json.loads(connection.get_extra())['account']

    # Connect to Snowflake and run query:
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account)
    cs = conn.cursor()
    try:
        cs.execute(snowflake_query)
        print("Executing query...")
        df = cs.fetch_pandas_all()
        print("Writing parquet...")
        df.to_parquet(file_name)
        print("Wrote parquet!")
    finally:
        cs.close()
    conn.close()

    # This gets output in the logs:
    return f"Successfully queried snowflake"

def send_to_s3(ds, **kwargs):
    # Upload parquet file to S3:
    s3 = boto3.resource('s3')
    destination_key = file_name
    s3.meta.client.upload_file(file_name, bucket_name, destination_key)

    # This gets output in the logs:
    return f"Successfully uploaded {file_name} to {bucket_name}"

task1 = PythonOperator(
    dag=dag,
    provide_context=True,
    task_id='pull_from_snowflake',
    python_callable=pull_from_snowflake,
    op_kwargs={}
)

task2 = PythonOperator(
    dag=dag,
    provide_context=True,
    task_id='send_to_s3',
    python_callable=send_to_s3,
    op_kwargs={}
)

task1 >> task2
