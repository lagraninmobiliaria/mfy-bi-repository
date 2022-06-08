import os

from google.cloud.bigquery import Client
from google.cloud.bigquery import LoadJobConfig

from dependencies.keys_and_constants import PROJECT_ID,DATASET_MUDATA_RAW, writeDisposition, createDisposition

def validate_search_reactivation_as_client_reactivation(closed_client_query: str, client_reactivation_query: str, **context):
    bq_client= Client(project= PROJECT_ID, location= 'us-central1')
    
    job_id= context['task_instance'].xcom_pull('query_daily_search_reactivation_events')
    bq_job= bq_client.get_job(job_id= job_id)
    query_results= bq_job.to_dataframe()
    
    append_rows = []
    record_existance_query_path = os.path.join(
        os.path.dirname(__file__),
        'queries',
        'reactivation_event_already_exists.sql'
    )
    with open(record_existance_query_path, 'r') as sql_file:
        record_existance_query = sql_file.read()

    for row in query_results.to_dict('records'):
        closed_client_query = closed_client_query.format(row.get('client_id'), row.get('created_at'))
        last_closed_client_event= bq_client.query(query= closed_client_query).result().to_dataframe().to_dict('records')
        
        if not len(last_closed_client_event):
            continue

        last_closed_client_event= last_closed_client_event.pop()

        client_last_reactivation_query = client_reactivation_query.format(row.get('client_id'), row.get('created_at'))
        last_client_last_reactivation_event = bq_client.query(query= client_last_reactivation_query).result().to_dataframe().to_dict('records')


        to_append_option_1 = not len(last_client_last_reactivation_event)
        to_append_option_2 = (
                len(last_client_last_reactivation_event)
            and last_closed_client_event.get('created_at') >= \
                last_client_last_reactivation_event[0].get('created_at') 
        )

        condition_1 = (to_append_option_1 or to_append_option_2)
        temp_query= record_existance_query.format(
            PROJECT_ID, 
            DATASET_MUDATA_RAW, 
            'client_reactivation_events',
            'event_id',
            row.get('event_id')
        )
        print(temp_query) 
        condition_2 = Client.query(
            query= temp_query
        ).result().num_results() == 0

        if condition_1 and condition_2:
            append_rows.append(row)

    print(append_rows)
    if len(append_rows):
        Client.load_table_from_json(
            json_rows= append_rows, 
            destination= f'{PROJECT_ID}.{DATASET_MUDATA_RAW}.client_reactivation_events',
            job_config= LoadJobConfig(
                create_disposition= createDisposition.CREATE_NEVER,
                write_disposition= writeDisposition.WRITE_APPEND
            )
        )