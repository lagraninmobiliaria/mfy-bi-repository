from datetime import datetime

from dependencies.keys_and_constants import PROJECT_ID, STG_DATASET_MUDATA_RAW

from include.dag_get_clients_info_on_creation.functions import get_clients_data

from airflow import DAG
from airflow.operators.dummy                            import DummyOperator
from airflow.operators.python                           import PythonOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator

import os

with DAG(
    dag_id= 'stg_get_clients_info_on_creation', 
    schedule_interval= '@daily',
    start_date= datetime(2020, 4, 13),
    end_date= datetime(2020, 4, 20),
    is_paused_upon_creation= True, 
    catchup= True,
    params= {
        'project_id': PROJECT_ID, 
        'mudata_raw': STG_DATASET_MUDATA_RAW
    }
) as dag:

    task_start_dag = DummyOperator(
        task_id= "start_dag"
    )

    QUERY_PATH_QE_DAY = './queries/first_client_qe_day.sql'

    task_query_first_client_qe_day = BigQueryInsertJobOperator(
        task_id= "query_first_client_question_events_of_day",
        configuration= {
            "query": {
                "query": f"{'{%'} include '{QUERY_PATH_QE_DAY}' {'%}'}",
                "useLegacySql": False,
            }
        }
    )

    task_get_clients_data = PythonOperator(
        task_id= "get_clients_data",
        python_callable= get_clients_data
    )

    task_end_dag = DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> task_query_first_client_qe_day >> task_get_clients_data >> task_end_dag