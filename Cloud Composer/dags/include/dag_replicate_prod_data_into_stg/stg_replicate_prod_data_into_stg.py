from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator

from google.cloud.bigquery import Client

bq_client= Client(project= 'infrastructure-lgi', location= 'us-central1')
list_tables= bq_client.list_tables(dataset= 'prod_mudata_raw')

with DAG(
    dag_id= 'stg_replicate_prod_data_into_stg',
    start_date= datetime(2021, 1, 1),
    schedule_interval= "@once"
) as dag:

    for table in list_tables:
        task= DummyOperator(
            task_id= f'transfer_data_{table}',
        )

