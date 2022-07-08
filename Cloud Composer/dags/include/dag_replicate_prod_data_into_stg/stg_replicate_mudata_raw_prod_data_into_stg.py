from datetime import datetime

from dependencies.keys_and_constants import PROD_DATASET_MUDATA_RAW_TABLES, STG_PARAMS, PROD_PARAMS

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator

from google.cloud.bigquery import WriteDisposition, CreateDisposition

list_tables= PROD_DATASET_MUDATA_RAW_TABLES

with DAG(
    dag_id= 'stg_replicate_mudata_raw_prod_data_into_stg',
    start_date= datetime(2021, 1, 1),
    schedule_interval= "@once",
    is_paused_upon_creation= True,
    catchup= True,
    params= {
        'stg_params': STG_PARAMS,
        'prod_params': PROD_PARAMS
    }
) as dag:

    for table in list_tables:
        task= BigQueryToBigQueryOperator(
            task_id= f"transfer_data_{table.table_id}",
            source_project_dataset_tables= '.'.join([
                "{{ params.prod_params['project_id'] }}", 
                "{{ params.prod_params['mudata_raw'] }}", 
                table.table_id
            ]),
            destination_project_dataset_table='.'.join([
                "{{ params.stg_params['project_id'] }}", 
                "{{ params.stg_params['mudata_raw'] }}",
                table.table_id
            ]),
            create_disposition= CreateDisposition.CREATE_IF_NEEDED,
            write_disposition= WriteDisposition.WRITE_TRUNCATE
        )

