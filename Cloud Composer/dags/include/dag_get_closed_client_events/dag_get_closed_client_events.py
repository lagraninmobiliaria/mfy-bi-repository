from datetime import datetime

from dependencies.keys_and_constants import PROJECT_ID, DATASET_MUDATA_RAW, writeDisposition, createDisposition

from google.cloud.bigquery.table import TimePartitioning, TimePartitioningType

from airflow                                            import DAG
from airflow.operators.dummy                            import DummyOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator

with DAG(
    dag_id= "get_closed_client_events",
    schedule_interval= "@daily",
    start_date= datetime(2021, 10, 19),
    is_paused_upon_creation= True,
    doc_md= "{% include ./markdowns/dag_get_closed_client_events.md %}",
    user_defined_macros= {
        "polymorphic_ctype_id": 120
    }
) as dag:

    task_start_dag = DummyOperator(
        task_id= 'start_dag'
    )

    SQL_QUERY_PATH= f'./queries/{dag.dag_id}.sql'
    table_id= 'closed_client_events'

    task_create_closed_client_events_table = BigQueryCreateEmptyTableOperator(
        task_id= 'create_closed_client_events_table',
        exists_ok= True,
        project_id= PROJECT_ID,
        dataset_id= DATASET_MUDATA_RAW,
        table_id= table_id,
        schema_fields= [
            {"name": "event_id", "type":"INTEGER" , "mode": "REQUIRED"},
            {"name": "created_at", "type":"TIMESTAMP" , "mode": "REQUIRED"},
            {"name": "client_id", "type":"INTEGER" , "mode": "REQUIRED"},
        ],
        time_partitioning= TimePartitioning(
            field= "created_at", 
            type_= TimePartitioningType.DAY
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
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_MUDATA_RAW,
                    "tableId": table_id
                },
                "writeDisposition": writeDisposition.WRITE_APPEND,
                "createDisposition": createDisposition.CREATE_NEVER
            }
        }
    )

    task_end_dag = DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> task_create_closed_client_events_table >> task_get_closed_client_events >> task_end_dag