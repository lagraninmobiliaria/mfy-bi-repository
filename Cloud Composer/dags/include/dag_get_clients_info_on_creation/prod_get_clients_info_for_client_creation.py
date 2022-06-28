from datetime import datetime

from dependencies.keys_and_constants import PROJECT_ID, PROD_DATASET_MUDATA_RAW

from include.dag_get_clients_info_on_creation.functions import get_clients_data

from airflow import DAG
from airflow.operators.dummy                            import DummyOperator
from airflow.operators.python                           import PythonOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator
from airflow.sensors.external_task                      import ExternalTaskSensor

with DAG(
    dag_id= 'prod_get_clients_info_on_creation', 
    schedule_interval= '@daily',
    start_date= datetime(2020, 4, 13),
    is_paused_upon_creation= True, 
    catchup= True,
    params= {
        'project_id': PROJECT_ID, 
        'mudata_raw': PROD_DATASET_MUDATA_RAW,
        'env_prefix': 'prod'
    }
) as dag:

    sensor_check_client_first_qe_successful_run= ExternalTaskSensor(
        task_id= 'check_client_first_qe_successful_run',
        poke_interval= 60,
        timeout= 60*5,
        external_dag_id= "{{ params.env_prefix }}" + "_get_client_first_question_events",
        external_task_id= "end_dag"
    )

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
                "jobReference": {
                    "projectId": "{{ params.project_id }}",
                    "location": 'us-central1'
                },
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

    sensor_check_client_first_qe_successful_run >> task_start_dag >> task_query_first_client_qe_day >> task_get_clients_data >> task_end_dag