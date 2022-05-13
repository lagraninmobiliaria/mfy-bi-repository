from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from datetime import datetime

from dependencies.keys_and_constants import PROJECT_ID, DATASET_MUDATA_RAW, writeDisposition, createDisposition
from include.sql import queries

default_args = {
    'retries': 0,
}


with DAG(
    dag_id= 'get_listed_and_unlisted_propertyevents.py',
    start_date= datetime(2021, 5, 3),   
    schedule_interval= '@daily',
    default_args= default_args,
    catchup= False,
) as dag:

    table_id= "test_table"
    sql = queries.test_bquery()

    test_task = BigQueryInsertJobOperator(
        gcp_conn_id= "bigquery_default",
        task_id= 'test_task',
        configuration= {
            "query": {
                "query": sql,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_MUDATA_RAW,
                    "tableId": table_id
                }, 
                "writeDisposition": writeDisposition.WRITE_APPEND,
                "createDisposition": createDisposition.CREATE_IF_NEEDED,
            }
        },
    )

    test_task