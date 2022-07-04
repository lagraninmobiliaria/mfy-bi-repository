from datetime import datetime

from dependencies.keys_and_constants import PROJECT_ID, PROD_DATASET_MUDATA_RAW

from google.cloud.bigquery.table import TimePartitioning, TimePartitioningType

from airflow                                            import DAG
from airflow.operators.dummy                            import DummyOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator

from google.cloud.bigquery import WriteDisposition, CreateDisposition

with DAG(
    dag_id= "prod_get_closed_client_events",
    schedule_interval= "@daily",
    start_date= datetime(2021, 10, 19),
    is_paused_upon_creation= True,
    params= {
        "polymorphic_ctype_id": 120,
        "project_id": PROJECT_ID,
        "mudata_raw": PROD_DATASET_MUDATA_RAW
    }
) as dag:

    task_start_dag = DummyOperator(
        task_id= 'start_dag'
    )

    SQL_QUERY_PATH= f'./queries/{dag.dag_id.split(sep="_", maxsplit= 1)[-1]}.sql'
    table_id= 'closed_client_events'

    task_create_closed_client_events_table = BigQueryCreateEmptyTableOperator(
        task_id= 'create_closed_client_events_table',
        exists_ok= True,
        project_id= "{{ params.project_id }}",
        dataset_id= "{{ params.mudata_raw }}",
        table_id= table_id,
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

    task_get_closed_client_events = BigQueryInsertJobOperator(
        task_id= 'get_closed_client_events',
        configuration= {
            "query": {
                "query": f"{'{%'} include '{SQL_QUERY_PATH}' {'%}'}",
                "useLegacySql": False,
                "jobReference": {
                    "projectId": PROJECT_ID
                },
                "destinationTable": {
                    "projectId": "{{ params.project_id }}",
                    "datasetId": "{{ params.mudata_raw }}",
                    "tableId": table_id
                },
                "writeDisposition": WriteDisposition.WRITE_APPEND,
                "createDisposition": CreateDisposition.CREATE_NEVER
            }
        }
    )

    task_end_dag = DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> task_create_closed_client_events_table >> task_get_closed_client_events >> task_end_dag