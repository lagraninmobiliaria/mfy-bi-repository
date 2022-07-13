from pendulum import datetime

from dependencies.keys_and_constants import STG_PARAMS

from include.dag_create_mudata_raw_tables.table_schemas import DEVELOPMENTS
from include.dag_get_developments.functions import build_query_to_get_developments

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

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

    task_build_query_to_get_developments= PythonOperator(
        task_id= "build_query_to_get_developments",
        python_callable= build_query_to_get_developments,
        op_kwargs= {
            'schema_fields': DEVELOPMENTS.get('schema_fields')
        }
    )

    task_end_dag= DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> task_build_query_to_get_developments >> task_end_dag

