from google.cloud.bigquery import Client

from dependencies.keys_and_constants import PROJECT_ID

def validate_search_reactivation_as_client_reactivation(closed_client_query: str, client_reactivation_query: str, **context):
    bq_client= Client(project= PROJECT_ID, location= 'us-central1')
    
    job_id= context['task_instance'].xcom_pull('query_daily_search_reactivation_events')
    bq_job= bq_client.get_job(job_id= job_id)
    query_results= bq_job.to_dataframe()
    
    append_rows = []

    for row in query_results.to_dict('records'):
        closed_client_query = closed_client_query.format(row.get('client_id'), row.get('created_at'))
        last_closed_client_event= bq_client.query(query= closed_client_query).result().to_dataframe().to_dict('records')
        
        if not len(last_closed_client_event):
            continue

        client_last_reactivation_query = client_reactivation_query.format(row.get('client_id'), row.get('created_at'))
        last_client_last_reactivation_event = bq_client.query(query= client_last_reactivation_query).result().to_dataframe().to_dict('records')

        last_closed_client_event= last_closed_client_event[0]

        to_append_option_1 = not len(last_client_last_reactivation_event)
        to_append_option_2 = (
                len(last_client_last_reactivation_event)
            and last_closed_client_event.get('created_at') >=
                last_client_last_reactivation_event[0].get('created_at') 
        )

        if to_append_option_1 or to_append_option_2:
            append_rows.append(row)

    print(append_rows)