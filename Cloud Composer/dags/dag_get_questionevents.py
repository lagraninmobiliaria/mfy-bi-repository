from datetime import datetime

from dependencies.keys_and_constants import DATASET_MUDATA_RAW, PROJECT_ID, writeDisposition, createDisposition

from airflow import DAG
from airflow.operators.dummy                            import DummyOperator
from airflow.operators.bash                             import BashOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator

with DAG(
    dag_id= 'get_questionevents',
    schedule_interval= '@daily',
    start_date= datetime(2020, 4, 8),
    max_active_runs= 5, 
    is_paused_upon_creation= True,
) as dag:

    date = "{{ ds }}"

    task_start_dag = BashOperator(
        task_id = 'start_dag',
        bash_command= f"echo Start DAGRun - Logical date: {date}"
    )

    table_id = 'questionevents'
    SQL_QUERY_PATH= './include/dag_get_questionevents/queries/get_questionevents.sql'

    task_get_questionevents = BigQueryInsertJobOperator(
        task_id= 'get_questionevents',
        configuration= {
            "query": {
                "query": f"{'{%'} include {SQL_QUERY_PATH} {'%}'}",
                "useLegacySql": False,
                 "jobReference": {
                    "projectId": PROJECT_ID,
                },
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_MUDATA_RAW,
                    "tableId": table_id
                }, 
                "writeDisposition": writeDisposition.WRITE_APPEND,
                "createDisposition": createDisposition.CREATE_IF_NEEDED,
            }
        }
    )

    task_end_dag = DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> task_get_questionevents >> task_end_dag