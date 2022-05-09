from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryDeleteTableOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

from datetime import datetime
from dependencies.keys_and_constants import DATASET_MUDATA_RAW, DATASET_MUDATA_CURATED, DATASET_MUDATA_AGGREGATED, PROJECT_ID

with DAG(
    dag_id= "runs_log_tables_creation",
    description= "The following DAG creates in each of the datasets a table to log runs of the DAGs created",
    schedule_interval= None, 
    start_date= datetime(2022, 1, 1),   
) as dag:

    start_task = DummyOperator(
        task_id= 'start',
    )

    tasks = []
    for dataset in [DATASET_MUDATA_RAW, DATASET_MUDATA_CURATED, DATASET_MUDATA_AGGREGATED]:
        task = BigQueryCreateEmptyTableOperator(
            task_id= 'table_creation_in_' + dataset,
            project_id= PROJECT_ID,
            dataset_id= dataset,
            table_id= 'dag_runs',
            trigger_rule= TriggerRule.ALL_SUCCESS,
            exists_ok= True, 
            schema_fields= [
                {"name": "run_id", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "date", "type": "DATE", "mode": "REQUIRED"},
                {"name": "dag_id", "type": "STRING", "mode": "REQUIRED"},
                {"name": "successful_run", "type": "BOOL", "mode": "REQUIRED"}
            ]
        )

        tasks.append(task)
    
    end_task = DummyOperator(
        task_id= 'finish'
    )

    start_task >> tasks >> end_task

with DAG(
    dag_id= 'run_logs_table_deletion',
    schedule_interval= None,
    start_date= datetime(2021, 1, 1),
    catchup= False
) as dag_2:

    start_task_2 = DummyOperator(
        task_id= 'start',
    )

    tasks_2 = []
    for dataset in [DATASET_MUDATA_RAW, DATASET_MUDATA_CURATED, DATASET_MUDATA_AGGREGATED]:
        task = BigQueryCreateEmptyTableOperator(
            task_id= 'table_creation_in_' + dataset,
            project_id= PROJECT_ID,
            dataset_id= dataset,
            table_id= 'dag_runs',
            trigger_rule= TriggerRule.ALL_SUCCESS,
            exists_ok= True, 
            schema_fields= [
                {"name": "run_id", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "date", "type": "DATE", "mode": "REQUIRED"},
                {"name": "dag_id", "type": "STRING", "mode": "REQUIRED"},
                {"name": "successful_run", "type": "BOOL", "mode": "REQUIRED"}
            ]
        )

        tasks_2.append(task)
    
    end_task_2 = DummyOperator(
        task_id= 'finish'
    )

    start_task_2 >> tasks_2 >> end_task_2