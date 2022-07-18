from webbrowser import get
from pendulum import datetime

from dependencies.keys_and_constants import PROD_PARAMS

from include.dag_create_mudata_raw_tables.table_schemas import DEVELOPMENTS
from include.dag_get_developments.functions import build_query_to_get_developments

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from google.cloud.bigquery import WriteDisposition, CreateDisposition

with DAG(
    dag_id= 'prod_get_developments',
    start_date= datetime(2021, 1, 1),
    schedule_interval= "@daily",
    is_paused_upon_creation= True,
    tags= ['production'], 
    catchup= False,
    params= PROD_PARAMS
) as dag:

    task_start_dag= DummyOperator(
        task_id= 'start_dag'
    )

    query_to_get_developments= build_query_to_get_developments(
        schema_fields= DEVELOPMENTS.get('schema_fields')
    )

    task_query_developments_from_mudafy_db= BigQueryInsertJobOperator(
        task_id= 'get_developments_from_mudafy_db',
        configuration= {
            "query": {
                "query": query_to_get_developments,
                "useLegacySql": False,
                 "jobReference": {
                    "projectId": "{{ params.project_id }}",
                },
                "destinationTable": {
                    "projectId": "{{ params.project_id }}",
                    "datasetId": "{{ params.mudata_raw }}",
                    "tableId": DEVELOPMENTS.get('table_id')
                }, 
                "writeDisposition": WriteDisposition.WRITE_TRUNCATE,
                "createDisposition": CreateDisposition.CREATE_NEVER,
            }
        }
    )


    task_end_dag= DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> task_query_developments_from_mudafy_db >> task_end_dag

