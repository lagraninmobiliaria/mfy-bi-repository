from datetime import datetime

from dependencies.keys_and_constants import DATASET_MUDATA_RAW, PROJECT_ID

from google.cloud.bigquery import WriteDisposition, CreateDisposition

from airflow import DAG
from airflow.operators.dummy                            import DummyOperator
from airflow.operators.bash                             import BashOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator

with DAG(
    dag_id= 'get_question_events',
    schedule_interval= '@daily',
    start_date= datetime(2020, 4, 8),
    max_active_runs= 5, 
    is_paused_upon_creation= True,
    user_defined_macros= {
        "polymorphic_ctype_id": 42
    }
) as dag:

    task_start_dag = BashOperator(
        task_id = 'start_dag',
        bash_command= f"echo Start DAGRun - Logical date: {{ ds }}"
    )

    SQL_QUERY_PATH= f'./queries/{dag.dag_id}.sql'
    table_id = 'question_events'

    task_get_question_events = BigQueryInsertJobOperator(
        task_id= 'get_question_events',
        configuration= {
            "query": {
                "query": f"{'{%'} include '{SQL_QUERY_PATH}' {'%}'}",
                "useLegacySql": False,
                 "jobReference": {
                    "projectId": PROJECT_ID,
                },
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_MUDATA_RAW,
                    "tableId": table_id
                }, 
                "writeDisposition": WriteDisposition.WRITE_APPEND,
                "createDisposition": CreateDisposition.CREATE_IF_NEEDED,
            }
        }
    )

    task_end_dag = DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> task_get_question_events >> task_end_dag