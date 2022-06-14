from datetime import datetime, timedelta

from dependencies.keys_and_constants import PROJECT_ID, DATASET_MUDATA_RAW, DATASET_MUDATA_CURATED, writeDisposition, createDisposition

from airflow                                            import DAG
from airflow.utils.trigger_rule                         import TriggerRule
from airflow.utils.state                                import TaskInstanceState
from airflow.sensors.python                             import PythonSensor
from airflow.sensors.external_task                      import ExternalTaskSensor
from airflow.operators.dummy                            import DummyOperator
from airflow.operators.python                           import BranchPythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator


def dag_start_validator(**context):
    prev_data_interval_start_success= context["prev_data_interval_start_success"]
    data_interval_start= context['data_interval_start']
    print(
        prev_data_interval_start_success, 
        data_interval_start,
        prev_data_interval_start_success == (data_interval_start - timedelta(days= 1))
    )

    return (
        prev_data_interval_start_success is None 
    or prev_data_interval_start_success == (data_interval_start - timedelta(days= 1))
    )
    

def is_first_run_sensor(**context):
    prev_data_interval_start_success = context.get('prev_data_interval_start_success')
    return prev_data_interval_start_success is None

def is_first_run(**context):
    prev_data_interval_start_success = context.get('prev_data_interval_start_success')
    print(prev_data_interval_start_success)
    if prev_data_interval_start_success is None:
        return ['create_client_first_question_events_table', 'append_clients_first_question_events']
    else:
        return 'append_clients_first_question_events'

with DAG(
    dag_id= 'get_client_first_question_events',
    schedule_interval= '@daily',
    start_date= datetime(2020, 4, 13),
    max_active_runs= 1,
    is_paused_upon_creation= True,
    user_defined_macros= {
        'DATASET_MUDATA_RAW': DATASET_MUDATA_RAW,
        'DATASET_MUDATA_CURATED': DATASET_MUDATA_CURATED
    },
    catchup= True
) as dag:
    
    tasK_dag_start_validator = ShortCircuitOperator(
        task_id= 'dag_start_validator',
        python_callable= dag_start_validator,
    )

    first_dag_run = PythonSensor(
        task_id= 'first_dag_run_sensor',
        python_callable= is_first_run_sensor,
        poke_interval=5,
        timeout= 15,
        mode= 'poke',
    )

    previous_dag_run_successful = ExternalTaskSensor(
        task_id= 'previous_dag_run_successful_sensor',
        external_dag_id= dag.dag_id,
        external_task_id= 'end_dag',
        execution_delta= timedelta(days= 1),
        allowed_states= [TaskInstanceState.SUCCESS],
        poke_interval= 30,
        timeout= 60,
        mode= 'reschedule'
    )

    start_dag = DummyOperator(
        task_id= 'start_dag',
        trigger_rule= TriggerRule.ONE_SUCCESS,
    )
    
    branch_task = BranchPythonOperator(
        task_id= 'task_is_first_run',
        python_callable= is_first_run,
    ) 

    table_id = 'clients_first_question_events'

    task_create_table = BigQueryCreateEmptyTableOperator(
        task_id= 'create_client_first_question_events_table',
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

    SQL_QUERY_PATH= f'./queries/{dag.dag_id}.sql'

    task_append_clients_first_question_events = BigQueryInsertJobOperator(
        task_id= 'append_clients_first_question_events',
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

    [first_dag_run, previous_dag_run_successful] >> start_dag
    start_dag >> branch_task >> [task_create_table, task_append_clients_first_question_events]
    task_create_table  >> task_append_clients_first_question_events >> end_dag