import os

from datetime import datetime

from dependencies.keys_and_constants import PROJECT_ID, STG_DATASET_MUDATA_RAW

from include.dag_fill_closed_client_events_table.functions import load_inferred_closed_client_events

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from google.cloud.bigquery import WriteDisposition, CreateDisposition

with DAG(
    dag_id= 'stg_fill_closed_client_events',
    start_date= datetime(2021, 1, 1),
    schedule_interval= None,
    catchup= False,
    is_paused_upon_creation= True,
    tags= ['production'],
    params= {
        'project_id': PROJECT_ID,
        'mudata_raw': STG_DATASET_MUDATA_RAW,
        'env_prefix': 'stg'
    }
) as dag:
    
    task_start_dag= DummyOperator(
        task_id= "start_dag"
    )

    get_inferred_closed_client_events_query_path= os.path.join(   
        os.path.dirname(__file__),
        'queries',
        'get_inferred_closed_client_events.sql'        
    )

    with open(get_inferred_closed_client_events_query_path, 'r') as sql_file:
        get_inferred_closed_client_events_query= sql_file.read().format(
            project_id= "{{ params.project_id }}",
            dataset_id= "{{ params.mudata_raw }}",
            table_id= "closed_client_events"
        )

    task_get_inferred_closed_client_events= BigQueryInsertJobOperator(
        task_id= 'get_inferred_closed_client_events',
        configuration= {
            "query": {
                "query": get_inferred_closed_client_events_query,
                "useLegacySql": False
            }
        }
    )

    task_load_inferred_closed_client_events= PythonOperator(
        task_id= "load_inferred_closed_client_events",
        python_callable= load_inferred_closed_client_events
    )

    task_end_dag= DummyOperator(
        task_id= "end_dag"
    )
    

    task_start_dag >> task_get_inferred_closed_client_events >> task_load_inferred_closed_client_events >> task_end_dag