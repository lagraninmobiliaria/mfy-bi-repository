from pendulum import datetime

from dependencies.keys_and_constants import STG_PARAMS

from include.dag_create_mudata_raw_tables.table_schemas import DEVELOPMENTS
from include.dag_get_developments.functions import build_query_to_get_developments

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

with DAG(
    dag_id= 'stg_get_developments',
    start_date= datetime(2021, 1, 1),
    schedule_interval= "@once",
    is_paused_upon_creation= True,
    tags= ['staging'], 
    catchup= False,
    params= STG_PARAMS
) as dag:

    task_start_dag= DummyOperator(
        task_id= 'start_dag'
    )

    query_to_get_developments= build_query_to_get_developments(
        schema_fields= DEVELOPMENTS.get('schema_fields')
    )

    task_get_developments_from_mudafy_db= BigQueryInsertJobOperator(
        task_id= 'get_developments_from_mudafy_db',
        configuration= {
            "query": {
                "query": query_to_get_developments,
                "useLegacySql": False
            }
        }
    )

    task_end_dag= DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> task_get_developments_from_mudafy_db >> task_end_dag

