from datetime import datetime, timedelta

from dependencies.keys_and_constants import DATASET_MUDATA_RAW, createDisposition, writeDisposition, PROJECT_ID

from include.dag_get_client_reactivation_events.functions import validate_search_reactivation_as_client_reactivation

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator

with DAG(
    dag_id= 'get_client_reactivation_events',
    schedule_interval= '@daily',
    max_active_runs= 1, 
    start_date= datetime(2021, 10, 19),
    end_date= datetime(2021, 10, 19) + timedelta(days= 10),
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

    SQL_QUERY_PATH_CLOSED_CLIENT= './queries/client_last_closed_event.sql'
    SQL_QUERY_PATH_CLIENT_REACTIVATION= './queries/client_last_reactivation_event.sql'

    task_search_reactivations_as_client_reactivations= PythonOperator(
        task_id= 'search_reactivations_as_client_reactivations',
        python_callable= validate_search_reactivation_as_client_reactivation,
        op_kwargs= {
            'closed_client_query': f"{'{%'} include '{SQL_QUERY_PATH_CLOSED_CLIENT}' {'%}'}",
            'client_reactivation_query': f"{'{%'} include '{SQL_QUERY_PATH_CLIENT_REACTIVATION}' {'%}'}",
        }
    )

    task_end_dag= DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> task_create_client_reactivation_table
    task_create_client_reactivation_table >> task_query_daily_search_reactivation_events >> task_search_reactivations_as_client_reactivations
    task_search_reactivations_as_client_reactivations >> task_end_dag
    
    