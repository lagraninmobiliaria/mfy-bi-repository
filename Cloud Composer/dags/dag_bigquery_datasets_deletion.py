from datetime import datetime, timedelta

from dependencies.keys_and_constants import DATASET_MUDATA_RAW, DATASET_MUDATA_CURATED, DATASET_MUDATA_AGGREGATED, PROJECT_ID

from airflow                                            import DAG
from airflow.utils.trigger_rule                         import TriggerRule
from airflow.operators.dummy                            import DummyOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryDeleteDatasetOperator

default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds= 60)
}

with DAG(
    dag_id= 'bigquery_datasets_deletion',
    start_date= datetime(2022, 3, 1),
    schedule_interval= None,
    default_args= default_args,
    is_paused_upon_creation= True,
) as dag:

    tasks = []

    task_start_dag= DummyOperator(
        task_id= 'start_dag'
    )

    for dataset in [DATASET_MUDATA_RAW, DATASET_MUDATA_CURATED, DATASET_MUDATA_AGGREGATED]:

        task = BigQueryDeleteDatasetOperator(
            task_id = 'delete_dataset_' + dataset,
            project_id= PROJECT_ID,
            dataset_id= dataset,
            delete_contents= True,
        )

        tasks.append(task)

    task_end_dag = DummyOperator(
        task_id= 'end_dag',
        trigger_rule= TriggerRule.ALL_SUCCESS
    )
    
    task_start_dag >> tasks >> task_end_dag