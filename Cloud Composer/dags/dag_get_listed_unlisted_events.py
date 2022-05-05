from airflow import DAG
from airflow.operators.dummy import DummyOperator 

from dependencies.keys_and_constants import PROJECT_ID, DATASET_MUDATA_CURATED, TRIGGER_RULES


with DAG(
        dag_id= 'data_warehouse_table_creation',
        schedule_interval= None
    ) as dag:
    
    task_1 = DummyOperator(
        task_id= 'task_query_posgresql',
        trigger_rule= TRIGGER_RULES.ALL_SUCCESS
    )

    task_2 = DummyOperator(
        task_id= 'task_update_table'
    )

    
if __name__ ==  "__main__":
    print(TRIGGER_RULES.ALL_DONE)
