from datetime import datetime

from dependencies.keys_and_constants import PROJECT_ID, PROD_DATASET_MUDATA_RAW, PROD_DATASET_MUDATA_CURATED

from airflow                                            import DAG
from airflow.sensors.external_task                      import ExternalTaskSensor
from airflow.operators.dummy                            import DummyOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator

from google.cloud.bigquery import WriteDisposition, CreateDisposition

with DAG(
    dag_id= 'prod_append_new_clients_to_look_clients_table',
    schedule_interval= '@daily',
    start_date= datetime(2020, 4, 13, 1, 30),
    max_active_runs=1,
    is_paused_upon_creation= True, 
    catchup= True,
    params= {
        'project_id': PROJECT_ID, 
        'mudata_raw': PROD_DATASET_MUDATA_RAW,
        'mudata_curated': PROD_DATASET_MUDATA_CURATED,
        'env_prefix': 'prod'
    }
) as dag:

    sensor_check_clients_creation_run= ExternalTaskSensor(
        task_id= 'check_daily_clients_creation_run',
        poke_interval= 30,
        timeout= 30*60,
        external_dag_id= "{{ params.env_prefix }}" + '_get_clients_info_on_creation',
        external_task_id= 'end_dag'
    )

    task_start_dag= DummyOperator(
        task_id= 'start_dag'
    )

    SQL_QUERY_PATH= './queries/get_daily_clients_creation.sql'

    task_query_new_clients_creation= BigQueryInsertJobOperator(
        task_id= 'query_new_clients_creation',
        configuration= {
            'query': {
                'query': f"{'{%'} include '{SQL_QUERY_PATH}' {'%}'}",
                'useLegacySql': False,
                "jobReference": {
                    "projectId": "{{ params.project_id }}",
                    "location": 'us-central1'
                },
                "destinationTable": {
                    "projectId": "{{ params.project_id }}",
                    "datasetId": "{{ params.mudata_curated }}",
                    "tableId": "look_clients"
                },
                "writeDisposition": WriteDisposition.WRITE_APPEND,
                "createDisposition": CreateDisposition.CREATE_NEVER,
            }
        }
    )

    task_end_dag= DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> task_query_new_clients_creation >> task_end_dag