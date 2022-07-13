from datetime import datetime

from dependencies.keys_and_constants import STG_DATASET_MUDATA_RAW, PROJECT_ID
from include.dag_create_mudata_raw_tables.table_schemas import *

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

tables_to_create= [
    CLIENTS_CREATION, CLIENTS_MAIL_SUBSCRIPTION, 
    BUYING_OPPORTUNITY_CASES, USER_ASSIGNMENTS,
    TASKS, TICKETS, DEVELOPMENTS
]

with DAG(
    dag_id= "stg_create_mudata_raw_tables",
    start_date= datetime(2022, 1, 1),
    schedule_interval= None,
    is_paused_upon_creation= True,
    tags= ['staging'],
    params= {
        'project_id': PROJECT_ID,
        'mudata_raw': STG_DATASET_MUDATA_RAW
    }
) as dag:

    task_start_dag = DummyOperator(
        task_id= "start_dag"
    )

    tasks_create_tables = []

    for table in tables_to_create:

        task = BigQueryCreateEmptyTableOperator(
            task_id= f"create_table_{table.get('table_id')}",
            project_id= "{{ params.project_id }}",
            dataset_id= "{{ params.mudata_raw }}",
            table_id= table.get("table_id"),
            schema_fields= table.get("schema_fields"),
            time_partitioning= table.get("time_partitioning"),
            exists_ok= True,
        )

        tasks_create_tables.append(task)

    task_end_dag = DummyOperator(
        task_id= "end_dag"
    )

    task_start_dag >> tasks_create_tables >> task_end_dag