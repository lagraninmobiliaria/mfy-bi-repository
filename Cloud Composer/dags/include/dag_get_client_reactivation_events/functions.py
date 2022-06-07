from google.cloud.bigquery import Client

from dependencies.keys_and_constants import PROJECT_ID

def validate_search_reactivation_as_client_reactivation(query: str, **context):
    bq_client= Client(project= PROJECT_ID, location= 'us-central1')
    
    job_id= context['task_instance'].xcom_pull('query_daily_search_reactivation_events')
    bq_job= bq_client.get_job(job_id= job_id)
    query_results= bq_job.to_dataframe()
    SQL_QUERY_PATH = './queries/client_last_closed_event.sql'
    for row in query_results.to_dict('records'):
        client_query = query.format(row.get('client_id'), row.get('created_at'))
        print(client_query)