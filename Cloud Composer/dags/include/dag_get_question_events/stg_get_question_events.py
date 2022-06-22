from datetime import datetime

from dependencies.keys_and_constants import STG_DATASET_MUDATA_RAW, PROJECT_ID, schemaTypes

from google.cloud.bigquery import WriteDisposition, CreateDisposition
from google.cloud.bigquery.table import TimePartitioningType

from airflow import DAG
from airflow.utils.trigger_rule                         import TriggerRule
from airflow.operators.dummy                            import DummyOperator
from airflow.operators.bash                             import BashOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator

with DAG(
    dag_id= 'stg_get_question_events',
    schedule_interval= '@daily',
    start_date= datetime(2020, 4, 8),
    max_active_runs= 5, 
    is_paused_upon_creation= True,
    params= {
        "polymorphic_ctype_id": 42,
        'project_id': PROJECT_ID,
        'mudata_raw': STG_DATASET_MUDATA_RAW
    }
) as dag:

    task_start_dag = BashOperator(
        task_id = 'start_dag',
        bash_command= f"echo Start DAGRun - Logical date: {{ data_interval_start.date() }}"
    )

    SQL_QUERY_PATH= f'./queries/{dag.dag_id}.sql'
    table_id = 'question_events'

    task_create_question_events_table = BigQueryCreateEmptyTableOperator(
        task_id= "create_question_events_table",
        trigger_rule= TriggerRule.ALL_SUCCESS,
        project_id= "{{ params.project_id }}",
        dataset_id= "{{ params.mudata_raw }}",
        schema_fields= [
            {"name": "event_id", "type": schemaTypes.INTEGER},
            {"name": "created_at", "type": schemaTypes.TIMESTAMP},
            {"name": "client_id", "type": schemaTypes.INTEGER},
            {"name": "opportunity_id", "type": schemaTypes.INTEGER},
        ],
        time_partitioning= {
            "field": "created_at",
            "type": TimePartitioningType.DAY
        },
        table_id= table_id,
        exists_ok= True
    )

    task_get_question_events = BigQueryInsertJobOperator(
        task_id= 'get_question_events',
        configuration= {
            "query": {
                "query": f"{'{%'} include '{SQL_QUERY_PATH}' {'%}'}",
                "useLegacySql": False,
                 "jobReference": {
                    "projectId": "{{ params.project_id }}",
                },
                "destinationTable": {
                    "projectId": "{{ params.project_id }}",
                    "datasetId": "{{ params.mudata_raw }}",
                    "tableId": table_id
                }, 
                "writeDisposition": WriteDisposition.WRITE_APPEND,
                "createDisposition": CreateDisposition.CREATE_NEVER,
            }
        }
    )

    task_end_dag = DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> task_create_question_events_table >> task_get_question_events >> task_end_dag