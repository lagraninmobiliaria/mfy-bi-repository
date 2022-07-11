from datetime import datetime

from dependencies.keys_and_constants import PROD_DATASET_MUDATA_CURATED_TABLES, STG_PARAMS, PROD_PARAMS, DAGTags

from airflow                    import DAG
from airflow.operators.dummy    import DummyOperator
from airflow.operators.python   import PythonOperator

from include.dag_replicate_prod_dataset_data_into_stg.functions import transfer_data

list_tables= PROD_DATASET_MUDATA_CURATED_TABLES

with DAG(
    dag_id= 'replicate_mudata_curated_prod_data_into_stg',
    start_date= datetime(2021, 1, 1),
    schedule_interval= None,
    is_paused_upon_creation= True,
    catchup= False,
    tags=[DAGTags.DATA_TRANSFER],
    params= {
        'stg_params': STG_PARAMS,
        'prod_params': PROD_PARAMS
    }
) as dag:

    task_start_dag= DummyOperator(
        task_id= 'start_dag'
    )

    transfer_tasks= []
    for table in list_tables:
        task= PythonOperator(
            task_id= f"transfer_data_{table.table_id}",
            python_callable= transfer_data,
            op_kwargs={
                'table_id': table.table_id
            }
        )

        transfer_tasks.append(task)

    task_end_dag= DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> transfer_tasks >> task_end_dag
