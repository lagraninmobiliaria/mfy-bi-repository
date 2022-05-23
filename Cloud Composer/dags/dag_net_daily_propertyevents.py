#TODO Align imports to make it look nicer

from datetime import datetime, timedelta

from dependencies.keys_and_constants import PROJECT_ID, DATASET_MUDATA_RAW, DATASET_MUDATA_CURATED, createDisposition, writeDisposition

from google.cloud.exceptions    import NotFound
from google.cloud.bigquery      import Client, LoadJobConfig
import pandas as pd

from airflow                                            import DAG
from airflow.utils.trigger_rule                         import TriggerRule
from airflow.utils.state                                import TaskInstanceState
from airflow.sensors.external_task                      import ExternalTaskSensor
from airflow.operators.python                           import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.operators.dummy                            import DummyOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator, BigQueryJob
from airflow.providers.google.cloud.hooks.bigquery      import BigQueryHook

from include.dag_net_daily_propertyevents import queries
from include.dag_net_daily_propertyevents.functions import net_daily_propertyevents, row_validation

default_args = {
    'retries': 2,
    'retry_delay': timedelta(seconds= 60),
    'max_active_runs': 1
}

def task_net_daily_propertyevents(ti):
    bq_client = Client(project= PROJECT_ID, location= 'US')
    
    job_id = ti.xcom_pull(task_ids= 'query_daily_propertyevents')
    bq_job = bq_client.get_job(job_id= job_id)    
    query_results = bq_job.to_dataframe()

    properties = query_results.prop_id.unique()

    data = []

    for prop in properties:
        prop_events_df = query_results[query_results.prop_id == prop].copy().reset_index(drop= True).sort_values(by= 'created_at', ascending= True)
        events_balance_result = net_daily_propertyevents(prop_df= prop_events_df)
        if events_balance_result != ():
            data.append((ti.execution_date.date(), prop) + events_balance_result)

    df_to_append = pd.DataFrame(data, columns= ['registered_date', 'prop_id', 'is_listing', 'is_unlisting'])

    bq_load_job = bq_client.load_table_from_dataframe(
        dataframe= df_to_append,
        destination= f"{PROJECT_ID}.{DATASET_MUDATA_CURATED}.properties_daily_net_propertyevents",
        job_config= LoadJobConfig(
            create_disposition= createDisposition.CREATE_IF_NEEDED,
            write_disposition= writeDisposition.WRITE_APPEND
        )
    )

    return bq_load_job.job_id


def branch_based_in_bigquery_table_existance(project_id, dataset_id, table_id):

    hook = BigQueryHook(
        gcp_conn_id= 'google_cloud_default',
    )

    if hook.table_exists(
        project_id= project_id, dataset_id= dataset_id, table_id= table_id
    ):
        return 'py_validate_net_propertyevents'
    else:
        return 'create_properties_listings_and_unlistings_table'


def create_table_with_properties_listings_and_unlistings(ti):

    bq_client = Client(project= PROJECT_ID)
    job_id = ti.xcom_pull(task_ids= 'query_daily_net_propertyevents')
    bq_job = bq_client.get_job(job_id= job_id)
    query_results = bq_job.to_dataframe()

    bq_load_job = bq_client.load_table_from_dataframe(
        project= PROJECT_ID,
        dataframe= query_results, 
        destination= f"{PROJECT_ID}.{DATASET_MUDATA_CURATED}.properties_listings_and_unlistings",
        job_config= LoadJobConfig(
            create_disposition= createDisposition.CREATE_IF_NEEDED,
            write_disposition= writeDisposition.WRITE_APPEND
        )
    )

    return bq_load_job.job_id

def append_net_propertyevents(**context):
    bq_client = Client(project= PROJECT_ID)
    job_id = context['ti'].xcom_pull(task_ids= 'query_daily_net_propertyevents')
    bq_job = bq_client.get_job(job_id= job_id)
    query_results = bq_job.to_dataframe()

    table_id = 'properties_listings_and_unlistings'

    bq_client.load_table_from_dataframe(
        dataframe= query_results,
        destination= {
            "projectId": PROJECT_ID,
            "datasetId": DATASET_MUDATA_CURATED,
            "tableId": table_id        
        },
        job_config= LoadJobConfig(
            create_disposition= createDisposition.CREATE_NEVER,
            write_disposition= writeDisposition.WRITE_APPEND
        )
    )

def task_validate_net_propertyevents(ti):
    
    bq_client = Client(project= PROJECT_ID)
    job_id = ti.xcom_pull(task_ids= 'query_daily_net_propertyevents')
    bq_job = bq_client.get_job(job_id= job_id)
    query_results = bq_job.to_dataframe()

    properties = list(query_results.prop_id.values)
    data = []

    for prop in properties:
        to_validate_row = dict(query_results[query_results.prop_id == prop].copy().reset_index(drop= True).iloc[0])
        validation_row = dict(bq_client.query(
                query= queries.get_prop_last_listing_unlisting_event(
                    prop_id= to_validate_row.get('prop_id'), 
                    project_id= PROJECT_ID, 
                    dataset_id= DATASET_MUDATA_CURATED
                ),
                project= PROJECT_ID
            ).to_dataframe().iloc[0]
        )
        print(
            f"validation_row:\n{validation_row}",
            f"to_validate_row:\n{to_validate_row}",
            sep= '\n'
        )

        if not validation_row['prop_id'] or row_validation(validation_row, to_validate_row):
            print("APPENDED")
            data.append(to_validate_row)
        else:
            print("NOT APPENDED")

    bq_load_job = bq_client.load_table_from_json(
        json_rows= data, 
        project= PROJECT_ID,
        dataframe= query_results, 
        destination= f"{PROJECT_ID}.{DATASET_MUDATA_CURATED}.properties_listings_and_unlistings",
        job_config= LoadJobConfig(
            create_disposition= createDisposition.CREATE_NEVER,
            write_disposition= writeDisposition.WRITE_APPEND
        )  
    )

    return bq_load_job.job_id

def is_first_dag_run(**context):

    print(
        context['ds'],
        context['dag'].start_date.date(),
        datetime.strptime(context['ds'], '%Y-%m-%d').date() == context['dag'].start_date.date(),
        sep= '\n'
    )
    return datetime.strptime(context['ds'], '%Y-%m-%d').date() == context['dag'].start_date.date()

with DAG(
    dag_id= 'net_daily_propertyevents',
    start_date= datetime(2021, 5, 3),
    end_date= datetime(2021, 5, 4),
    schedule_interval= '@daily',
    default_args= default_args,
    catchup= True,
) as dag:

    date = "{{ ds }}"

    first_dug_run = ShortCircuitOperator(
        task_id= 'first_dag_run',
        python_callable= is_first_dag_run,
    )

    previous_dag_run_successful = ExternalTaskSensor(
        execution_delta= timedelta(days=1),
        task_id= 'previous_dag_run_successful',
        external_dag_id= dag.dag_id,
        external_task_id= 'end_dag',
        allowed_states= [TaskInstanceState.SUCCESS],
        poke_interval= 30,
        timeout= 60,
        mode= 'reschedule'
    )

    start_dag = ExternalTaskSensor(
        task_id= 'start_dag',
        depends_on_past= True,
        external_dag_id= 'get_listed_and_unlisted_propertyevents',
        external_task_id= 'end_dag',
        trigger_rule= TriggerRule.ONE_SUCCESS,
        poke_interval= 60,
        timeout= 60 * 2,
        allowed_states= [TaskInstanceState.SUCCESS.value],
        mode= 'reschedule',
    )

    query_daily_propertyevents = BigQueryInsertJobOperator(
        task_id= 'query_daily_propertyevents',
        configuration= {
            "query": {
                "query": queries.get_daily_propertyevents(
                    project_id= PROJECT_ID, 
                    dataset= DATASET_MUDATA_RAW, 
                    date= date
                ),
                "useLegacySql": False,
                "jobReference": {
                    "projectId": PROJECT_ID,
                }
            }
        },
    )

    net_daily_propertyevents_py_op = PythonOperator(
        task_id= 'net_daily_propertyevents',
        python_callable= task_net_daily_propertyevents,
        trigger_rule= TriggerRule.ALL_SUCCESS
    )

    query_daily_net_propertyevents = BigQueryInsertJobOperator(
        task_id= 'query_daily_net_propertyevents',
        configuration= {
            "query": {
                "query": queries.get_properties_daily_net_propertyevents(
                    project_id= PROJECT_ID,
                    dataset= DATASET_MUDATA_CURATED,
                    date= date
                ),
                "useLegacySql": False,
            }
        }
    )

    check_table_existance = BranchPythonOperator(
        task_id= 'branch_based_on_table_existance',
        python_callable= branch_based_in_bigquery_table_existance,
        op_kwargs= {
            'project_id': PROJECT_ID,
            'dataset_id': DATASET_MUDATA_CURATED,
            'table_id': 'properties_listings_and_unlistings',
        }
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id= 'create_properties_listings_and_unlistings_table',
        project_id= PROJECT_ID,
        dataset_id= DATASET_MUDATA_CURATED,
        exists_ok= True,
        table_id= 'properties_listings_and_unlistings'
    )

    py_append_net_propertyevents = PythonOperator(
        task_id= 'append_net_propertyevents',
        python_callable= 'append_net_propertyevents'
    )

    py_validate_net_propertyevents = PythonOperator(
        task_id= 'validate_net_propertyevents',
        python_callable= task_validate_net_propertyevents,
        trigger_rule= TriggerRule.ALL_SUCCESS
    )

    end_dag = DummyOperator(
        task_id= 'end_dag',
        trigger_rule= TriggerRule.ONE_SUCCESS
    )

    [first_dug_run, previous_dag_run_successful] >> start_dag >> query_daily_propertyevents >> net_daily_propertyevents_py_op >> query_daily_net_propertyevents >> check_table_existance 
    check_table_existance  >> py_validate_net_propertyevents >> end_dag
    check_table_existance  >> create_table >> py_append_net_propertyevents >> end_dag