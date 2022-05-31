from datetime import datetime

from dependencies.keys_and_constants import PROJECT_ID, DATASET_MUDATA_RAW, DATASET_MUDATA_CURATED

from airflow                                            import DAG
from airflow.utils.dates                                import days_ago
from airflow.utils.trigger_rule                         import TriggerRule
from airflow.operators.dummy                            import DummyOperator
from airflow.operators.python                           import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator

from google.cloud.bigquery                              import SchemaField

def is_first_run(**context):
    prev_data_interval_start_success = context.get('prev_data_interval_start_success')
    print(prev_data_interval_start_success)
    if prev_data_interval_start_success is None:
        return ['branch_a', 'branch_b']
    else:
        return 'branch_b'

def test_query_template(query):
    print(query)

with DAG(
    dag_id= 'get_client_first_questionevent',
    schedule_interval= '*/30 * * * *',
    start_date= datetime(2020, 4, 8),
    end_date= datetime(2020, 4, 9),
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

    SQL_QUERY_PATH= './include/dag_get_client_first_questionevent/queries/get_client_first_questionevent.sql'

    task_test_query_template = PythonOperator(
        task_id= 'test_query_template',
        python_callable= test_query_template,
        op_kwargs= {
            'query': f"{'{%'} include '{SQL_QUERY_PATH}' {'%}'}"
        }
    )
    
    branch_task = BranchPythonOperator(
        task_id= 'task_is_first_run',
        python_callable= is_first_run,
    ) 

    task_branch_a = BigQueryCreateEmptyTableOperator(
        task_id= 'branch_a',
        project_id= PROJECT_ID,
        dataset_id= DATASET_MUDATA_CURATED,
        table_id= 'clients_first_questionevent',
        schema_fields= [
            SchemaField('client_id', 'INTEGER'), 
            SchemaField('event_id', 'INTEGER'),
            SchemaField('created_at', 'TIMESTAMP'),
            SchemaField('opportunity_id', 'INTEGER'),
        ],
        time_partitioning= {'created_at', 'TIMESTAMP'},
        exists_ok= True,
    )

    task_branch_b = DummyOperator(
        task_id= 'branch_b'
    )
    end_dag = DummyOperator(
        task_id= 'end_dag',
        trigger_rule= TriggerRule.ONE_SUCCESS
    )

    start_dag >> branch_task >> [task_branch_a, task_branch_b]
    task_branch_a >> task_branch_b >> end_dag