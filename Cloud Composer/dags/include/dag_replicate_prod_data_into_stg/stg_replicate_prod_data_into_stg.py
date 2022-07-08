from datetime import datetime
from turtle import back

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator
from dependencies.keys_and_constants import STG_DATASET_MUDATA_RAW_TABLES

list_tables= STG_DATASET_MUDATA_RAW_TABLES

with DAG(
    dag_id= 'stg_replicate_prod_data_into_stg',
    start_date= datetime(2021, 1, 1),
    schedule_interval= "@once"
) as dag:

    BashOperator(
        bash_command= f"echo {[table.table_id for table in list_tables]}"
    )

