from datetime import datetime
from turtle import end_fill

import py

from dependencies.keys_and_constants import DATASET_MUDATA_RAW, createDisposition, writeDisposition, PROJECT_ID

from .functions import get_clients_with_search_reactivation_event, branching_based_on_results

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator

with DAG(
    dag_id= 'get_client_reactivation_events',
    schedule_interval= '@daily',
    max_active_runs= 1, 
    start_date= datetime(2021, 1, 1),
    params= {
        'project_id': PROJECT_ID,
        'mudata_raw': DATASET_MUDATA_RAW
    },
    is_paused_upon_creation= True
) as dag:
    
    task_start_dag = DummyOperator(
        task_id= 'start_dag'
    )


    task_create_client_reactivation_table = BigQueryCreateEmptyTableOperator(
        task_id= 'create_client_reactivation_events_table',
        exists_ok= True,
        project_id= PROJECT_ID,
        dataset_id= DATASET_MUDATA_RAW,
        table_id= 'client_reactivation_events',
        schema_fields= [
            {'name': 'event_id', 'type': 'INTEGER'},
            {'name': 'created_at', 'type': 'TIMESTAMP'},
            {'name': 'client_id', 'type': 'INTEGER'}
        ]
    )

    SQL_QUERY_PATH= f'./queries/daily_search_reactivation_events.sql'

    task_query_daily_search_reactivation_events = BigQueryInsertJobOperator(
        task_id= 'query_daily_search_reactivation_events',
        configuration= {
            "query": {
                "query": f"{'{%'} include '{SQL_QUERY_PATH}' {'%}'}",
                "useLegacySql": False,
            }
        }
    )

    task_get_clients_with_search_reactivation_event = PythonOperator(
        task_id= 'get_clients_with_search_reactivation_event',
        python_callable= get_clients_with_search_reactivation_event
    )

    # task_branching_based_on_results = BranchPythonOperator(
    #     task_id= 'branching_based_on_results',
    #     python_callable= branching_based_on_results,
    # )

    # with TaskGroup() as client_processing_group:
    #     # for client_id in 
    #     pass

    task_end_dag= DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> task_create_client_reactivation_table
    task_create_client_reactivation_table >> task_query_daily_search_reactivation_events >> task_get_clients_with_search_reactivation_event
    task_get_clients_with_search_reactivation_event >> task_end_dag
    
    