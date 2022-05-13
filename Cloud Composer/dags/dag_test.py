from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from datetime import datetime

from dependencies.keys_and_constants import PROJECT_ID, DATASET_MUDATA_RAW, writeDisposition, createDisposition
from include.sql import queries

default_args = {
    'retries': 0,
}


with DAG(
    dag_id= 'dag_test.py',
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
        location= 'us',
        configuration= {
            "query": {
                "query": sql,
                "useLegacySql": False,
                "jobReference": {
                    "projectId": PROJECT_ID,
                },
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": 'test_dataset',
                    "tableId": table_id,
                }, 
                "writeDisposition": writeDisposition.WRITE_APPEND,
                "createDisposition": createDisposition.CREATE_IF_NEEDED,
            }
        },
    )

    test_task