#TODO Change branch_b task name into something explicit about the task performed

from datetime import datetime

from dependencies.keys_and_constants import PROJECT_ID, DATASET_MUDATA_RAW, DATASET_MUDATA_CURATED, writeDisposition, createDisposition

from airflow                                            import DAG
from airflow.utils.dates                                import days_ago
from airflow.utils.trigger_rule                         import TriggerRule
from airflow.operators.dummy                            import DummyOperator
from airflow.operators.python                           import BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator

def is_first_run(**context):
    prev_data_interval_start_success = context.get('prev_data_interval_start_success')
    print(prev_data_interval_start_success)
    if prev_data_interval_start_success is None:
        return ['create_client_first_questionevent_table', 'branch_b']
    else:
        return 'branch_b'

with DAG(
    dag_id= 'get_client_first_questionevent',
    schedule_interval= '*/30 * * * *',
    start_date= datetime(2020, 4, 13, 15, 30),
    end_date= datetime(2020, 4, 14),
    max_active_runs= 1,
    is_paused_upon_creation= True,
    user_defined_macros= {
        'DATASET_MUDATA_RAW': DATASET_MUDATA_RAW,
        'DATASET_MUDATA_CURATED': DATASET_MUDATA_CURATED
    },
    catchup= True
) as dag:
    
    start_dag = DummyOperator(
        task_id= 'start_dag',
    )
    
    branch_task = BranchPythonOperator(
        task_id= 'task_is_first_run',
        python_callable= is_first_run,
    ) 

    table_id = 'clients_first_questionevent'

    task_create_table = BigQueryCreateEmptyTableOperator(
        task_id= 'create_client_first_questionevent_table',
        project_id= PROJECT_ID,
        dataset_id= DATASET_MUDATA_CURATED,
        table_id= table_id,
        schema_fields= [
            {'name': 'client_id', 'type':  'INTEGER'}, 
            {'name': 'event_id', 'type': 'INTEGER'},
            {'name': 'opportunity_id', 'type': 'INTEGER'},
            {'name': 'created_at', 'type': 'TIMESTAMP'},
        ],
        exists_ok= True,
    )

    SQL_QUERY_PATH= './include/dag_get_client_first_questionevent/queries/get_client_first_questionevent.sql'

    task_branch_b = BigQueryInsertJobOperator(
        task_id= 'branch_b',
        configuration= {
            "query": {
                "query": f"{'{%'} include '{SQL_QUERY_PATH}' {'%}'}",
                "useLegacySql": False,
                "jobReference": {
                    "projectId": PROJECT_ID,
                },
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_MUDATA_CURATED,
                    "tableId": table_id
                }, 
                "writeDisposition": writeDisposition.WRITE_APPEND,
                "createDisposition": createDisposition.CREATE_NEVER,
            }
        },
        project_id= PROJECT_ID,
        trigger_rule= TriggerRule.ONE_SUCCESS
    )

    end_dag = DummyOperator(
        task_id= 'end_dag',
        trigger_rule= TriggerRule.ALL_SUCCESS
    )

    start_dag >> branch_task >> [task_create_table, task_branch_b]
    task_create_table  >> task_branch_b >> end_dag