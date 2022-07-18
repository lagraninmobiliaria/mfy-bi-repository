import os

from datetime import datetime

from dependencies.keys_and_constants import STG_PARAMS

from google.cloud.bigquery import WriteDisposition, CreateDisposition

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
with DAG(
    dag_id= 'stg_update_look_developments',
    schedule_interval= None,
    start_date= datetime(2021, 1, 1),
    tags= ['staging'],
    is_paused_upon_creation= True,
    catchup= False,
    params= STG_PARAMS
) as dag:
    
    sensor_check_get_developments_successful_dagrun= ExternalTaskSensor(
        task_id= 'check_get_developments_successful_dagrun',
        poke_interval= 30,
        timeout= 30*5,
        external_dag_id= "{{ params.env_prefix }}" + "_get_developments",
        external_task_id= "end_dag"
    )

    task_start_dag= DummyOperator(
        task_id= 'start_dag'
    )

    update_look_developments_query_path= os.path.join(
        os.path.dirname(__file__),
        'queries',
        'update_look_developments.sql'
    )

    with open(update_look_developments_query_path, 'r') as sql_file:
        update_look_developments_query= sql_file.read().format(
            project_id= "{{ params.project_id }}",
            dataset_id= "{{ params.mudata_raw }}"
        )

    task_update_look_developments= BigQueryInsertJobOperator(
        task_id= 'update_look_developments',
        configuration= {
            "query": {
                "query": update_look_developments_query,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "{{ params.project_id }}",
                    "datasetId": "{{ params.mudata_curated }}",
                    "tableId": "look_developments"
                }, 
                "writeDisposition": WriteDisposition.WRITE_TRUNCATE,
                "createDisposition": CreateDisposition.CREATE_NEVER,
            }
        }
    )

    task_end_dag= DummyOperator(
        task_id= 'end_dag'
    )

    sensor_check_get_developments_successful_dagrun >> task_start_dag >> task_update_look_developments >> task_end_dag