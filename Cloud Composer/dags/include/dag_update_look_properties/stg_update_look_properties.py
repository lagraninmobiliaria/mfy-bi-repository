from datetime import datetime

from dependencies.keys_and_constants import STG_DATASET_MUDATA_CURATED, STG_DATASET_MUDATA_RAW, PROJECT_ID

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

with DAG(
    dag_id= 'stg_update_look_properties',
    schedule_interval= None,
    start_date= datetime(2021, 1, 1),
    params= {
        'mudata_raw': STG_DATASET_MUDATA_RAW,
        'mudata_curated': STG_DATASET_MUDATA_CURATED,
        'project_id': PROJECT_ID
    },
    is_paused_upon_creation= True,
) as dag:

    start_dag = DummyOperator(
        task_id= 'start_dag'
    )

    update_look_properties = BigQueryInsertJobOperator(
        task_id= 'update_look_properties',
        configuration= {
            "query": {
                "query": f"{'{%'} include './queries/update_look_properties.sql' {'%}'}",
                "useLegacySql": False
            }
        }
    )

    end_dag= DummyOperator(
        task_id= 'end_dag'
    )

    start_dag >> update_look_properties >> end_dag