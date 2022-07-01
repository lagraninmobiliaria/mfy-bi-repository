from datetime import datetime

from dependencies.keys_and_constants import STG_DATASET_MUDATA_RAW, PROJECT_ID

from airflow                                                import DAG
from airflow.utils.trigger_rule                             import TriggerRule
from airflow.operators.dummy                                import DummyOperator
from airflow.operators.bash                                 import BashOperator
from airflow.providers.google.cloud.operators.bigquery      import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator

from google.cloud.bigquery import WriteDisposition, CreateDisposition, TimePartitioningType

with DAG(
    dag_id= 'stg_get_search_reactivation_events',
    schedule_interval= '@daily',
    start_date= datetime(2021, 10, 19),
    end_date= datetime(2021, 11, 1),
    max_active_runs= 1,
    is_paused_upon_creation= True,
    params= {
        "polymorphic_ctype_id": 94,
        "project_id": PROJECT_ID,
        "mudata_raw": STG_DATASET_MUDATA_RAW
    }
) as dag:

    task_start_dag = BashOperator(
        task_id= 'start_dag',
        bash_command= "echo DAGRun started: {{ data_interval_start }} - {{ data_interval_end }}"
    )

    SQL_QUERY_PATH= f'./queries/{dag.dag_id.split(sep="_", maxsplit= 1)[-1]}.sql'
    destionation_table_id= 'search_reactivation_events'

    task_create_search_reactivation_events_table = BigQueryCreateEmptyTableOperator(
        task_id= 'create_search_reactivation_events_table',
        exists_ok= True,
        project_id= "{{ params.project_id }}",
        dataset_id= "{{ params.mudata_raw }}",
        table_id= destionation_table_id,
        schema_fields= [
            {"name": "event_id", "type":"INTEGER" , "mode": "REQUIRED"},
            {"name": "created_at", "type":"TIMESTAMP" , "mode": "REQUIRED"},
            {"name": "client_id", "type":"INTEGER" , "mode": "REQUIRED"},
        ],
        time_partitioning= dict(
            field= "created_at", 
            type= TimePartitioningType.DAY
        ),
        cluster_fields= ["client_id", "event_id"],
    )


    task_get_search_reactivation_events = BigQueryInsertJobOperator(
        task_id= 'get_search_reactivation_events',
        configuration= {
            "query": {
                "query": f"{'{%'} include '{SQL_QUERY_PATH}' {'%}'}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "{{ params.project_id }}",
                    "datasetId": "{{ params.mudata_raw }}",
                    "tableId": destionation_table_id
                },
                "jobReference": {
                    "projectId": PROJECT_ID,
                    "location": 'us-central1'
                },
                "writeDisposition": WriteDisposition.WRITE_APPEND,
                "createDisposition": CreateDisposition.CREATE_NEVER
            }
        }
    )

    task_end_dag = DummyOperator(
        task_id= 'end_dag',
        trigger_rule= TriggerRule.ALL_SUCCESS
    )

    task_start_dag >> task_create_search_reactivation_events_table >> task_get_search_reactivation_events >> task_end_dag
