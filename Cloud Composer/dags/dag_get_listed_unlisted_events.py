from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from datetime import datetime

from dependencies.keys_and_constants import PROJECT_ID, DATASET_MUDATA_CURATED, TRIGGER_RULES


with DAG(
        dag_id= 'dag_get_listed_unlisted_events',
        schedule_interval= None,
        start_date= datetime(2021, 1, 1),
        catchup= False
    ) as dag:



    task_1 = DummyOperator(
        task_id= 'task_query_posgresql',
        trigger_rule= TriggerRule.ALL_SUCCESS
    )


    task_2 = DummyOperator(
        task_id= 'task_update_table',
        trigger_rule= TriggerRule.ALL_SUCCESS
    )

    task_1 >> task_2

with DAG(
    dag_id= 'dag_test',
    schedule_interval= None,
    start_date= datetime(2021, 1, 1),
    catchup= False
) as dag:

    bigquery_table_existance = BigQueryTableExistenceSensor(
        task_id= 'bigquery_table_existance',
        project_id= PROJECT_ID, 
        dataset_id= 'DW_Mudafy',
        table_id= 'FCT_Busquedas_pero_no_existe',
    )

    table_exists = DummyOperator(
        task_id= 'table_exists',
        trigger_rule= TriggerRule.ALL_SUCCESS
    )

    table_doesnt_exists = DummyOperator(
        task_id= 'table_doesnt_exist',
        trigger_rule= TriggerRule.ALL_FAILED
    )

    bigquery_table_existance >> [table_exists, table_doesnt_exists]