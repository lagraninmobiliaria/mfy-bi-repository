from google.cloud import bigquery

from dependencies.keys_and_constants import PROJECT_ID

def get_clients_with_search_reactivation_event(**context):
    bq_client= bigquery.Client(project= PROJECT_ID)
    bq_job= bq_client.get_job(job_id= context['task_instance'].xcom_pull('query_daily_search_reactivation_events'))
    query_results= bq_job.to_dataframe()
    output = list(query_results.client_id.unique())
    context['task_instance'].xcom_push(key= 'list_client_id', value= output)