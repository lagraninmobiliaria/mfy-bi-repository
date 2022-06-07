from google.cloud.bigquery import Client

from dependencies.keys_and_constants import PROJECT_ID

def get_clients_with_search_reactivation_event(**context):
    bq_client= Client(project= PROJECT_ID, location= 'us-central1')
    job_id= context['task_instance'].xcom_pull('query_daily_search_reactivation_events')
    print(job_id)
    bq_job= bq_client.get_job(job_id= job_id)
    query_results= bq_job.to_dataframe()
    output = list(query_results.client_id.unique())
    print(output)
    context['task_instance'].xcom_push(key= 'list_client_id', value= output)