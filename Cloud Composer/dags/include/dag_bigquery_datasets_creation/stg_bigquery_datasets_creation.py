from datetime import datetime, timedelta

from dependencies.keys_and_constants import STG_DATASET_MUDATA_RAW, STG_DATASET_MUDATA_CURATED, STG_DATASET_MUDATA_AGGREGATED, PROJECT_ID

from airflow                                            import DAG
from airflow.utils.trigger_rule                         import TriggerRule
from airflow.operators.dummy                            import DummyOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryCreateEmptyDatasetOperator

default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds= 60)
}

with DAG(
    dag_id= 'stg_bigquery_datasets_creation',
    start_date= datetime(2022, 3, 1),
    schedule_interval= None,
    default_args= default_args,
    is_paused_upon_creation= True,
    params= {
        'project_id': PROJECT_ID,
        'mudata_raw': STG_DATASET_MUDATA_RAW,
        'mudata_curated': STG_DATASET_MUDATA_CURATED,
        'mudata_aggregated': STG_DATASET_MUDATA_AGGREGATED
    }  
) as dag:

    tasks = []

    start_task = DummyOperator(
        task_id= 'start_dag'
    )

    for dataset in ["{{ params.mudata_raw }}", "{{ params.mudata_curated }}", "{{ params.mudata_aggregated }}"]:

        task = BigQueryCreateEmptyDatasetOperator(
            task_id = 'create_dataset_' + dataset,
            project_id= "{{ params.project_id }}",
            dataset_id= dataset,
            location= 'us-central1',
            exists_ok= True
        )

        tasks.append(task)

    end_task = DummyOperator(
        task_id= 'end_dag',
        trigger_rule= TriggerRule.ALL_SUCCESS
    )
    
    start_task >> tasks >> end_task