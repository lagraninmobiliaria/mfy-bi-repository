from airflow import DAG
from airflow.operators.dummy import DummyOperator 

from datetime import datetime
from dependencies.keys_and_constants import PROJECT_ID, DATASET_MUDATA_CURATED, TRIGGER_RULES


with DAG(
        dag_id= 'dag_get_listed_unlisted_events',
        schedule_interval= None,
        start_date= datetime(2021, 1, 1),
    ) as dag:
    
    task_1 = DummyOperator(
        task_id= 'task_query_posgresql',
        trigger_rule= TRIGGER_RULES.ALL_SUCCESS
    )

    task_2 = DummyOperator(
        task_id= 'task_update_table',
        triger_rule= TRIGGER_RULES.ALL_SUCCESS
    )

    task_1 >> task_2