from datetime import datetime, timedelta

from dependencies.keys_and_constants                    import PROJECT_ID, PROD_DATASET_MUDATA_RAW, PROD_DATASET_MUDATA_CURATED

from airflow                                            import DAG
from airflow.utils.trigger_rule                         import TriggerRule
from airflow.operators.dummy                            import DummyOperator
from airflow.operators.python                           import BranchPythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from airflow.sensors.external_task                      import ExternalTaskSensor

from google.cloud.bigquery                              import WriteDisposition, CreateDisposition
from google.cloud.bigquery.table                        import TimePartitioningType


def dag_start_validator(**context):
    prev_data_interval_start_success= context["prev_data_interval_start_success"]
    data_interval_start= context['data_interval_start']

    return (
            prev_data_interval_start_success is None 
        or  prev_data_interval_start_success == (data_interval_start - timedelta(days= 1))
    )

def is_first_run(**context):
    prev_data_interval_start_success = context.get('prev_data_interval_start_success')
    print(prev_data_interval_start_success)
    if prev_data_interval_start_success is None:
        return ['create_client_first_question_events_table', 'append_clients_first_question_events']
    else:
        return 'append_clients_first_question_events'

with DAG(
    dag_id= 'prod_get_client_first_question_events',
    schedule_interval= '@daily',
    start_date= datetime(2020, 4, 13),
    max_active_runs= 1,
    is_paused_upon_creation= True,
    tags= ['production'],
    params= {
        'project_id': PROJECT_ID,
        'mudata_raw': PROD_DATASET_MUDATA_RAW,
        'env_prefix': 'prod'
    },
    catchup= True
) as dag:

    sensor_check_question_events= ExternalTaskSensor(
        task_id= 'check_question_events',
        external_dag_id= '{{ params.env_prefix }}' + '_get_question_events',
        external_task_id= 'end_dag',
        poke_interval= 60,
        timeout= 60*5
    )
    
    task_dag_start_validator = ShortCircuitOperator(
        task_id= 'dag_start_validator',
        python_callable= dag_start_validator,
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
        project_id= "{{ params.project_id }}",
        dataset_id= "{{ params.mudata_raw }}",
        table_id= table_id,
        schema_fields= [
            {'name': 'client_id',       'type':  'INTEGER'}, 
            {'name': 'event_id',        'type': 'INTEGER'},
            {'name': 'opportunity_id',  'type': 'INTEGER'},
            {'name': 'created_at',      'type': 'TIMESTAMP'},
        ],
        cluster_fields= ['client_id'],
        time_partitioning= {
            'field': 'created_at',
            'type': TimePartitioningType.DAY,
        },
        exists_ok= True,
    )

    SQL_QUERY_PATH= f'./queries/{dag.dag_id.split(maxsplit=1, sep="_")[-1]}.sql'

    task_append_clients_first_question_events = BigQueryInsertJobOperator(
        task_id= 'append_clients_first_question_events',
        configuration= {
            "query": {
                "query": f"{'{%'} include '{SQL_QUERY_PATH}' {'%}'}",
                "useLegacySql": False,
                "jobReference": {
                    "projectId": "{{ params.project_id }}",
                },
                "destinationTable": {
                    "projectId": "{{ params.project_id }}",
                    "datasetId": "{{ params.mudata_raw }}",
                    "tableId": table_id
                }, 
                "writeDisposition": WriteDisposition.WRITE_APPEND,
                "createDisposition": CreateDisposition.CREATE_NEVER,
            }
        },
        trigger_rule= TriggerRule.ONE_SUCCESS
    )

    end_dag = DummyOperator(
        task_id= 'end_dag',
        trigger_rule= TriggerRule.ALL_SUCCESS
    )

    sensor_check_question_events >> task_dag_start_validator >> start_dag
    start_dag >> branch_task >> [task_create_table, task_append_clients_first_question_events]
    task_create_table  >> task_append_clients_first_question_events >> end_dag