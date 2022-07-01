import os

from pandas import DataFrame

from google.cloud.bigquery import Client
from google.cloud.bigquery import LoadJobConfig

from google.cloud.bigquery import WriteDisposition, CreateDisposition

class DAGQueriesManager:
    
    def __init__(self):

        client_last_closed_event_query_path= os.path.join(
            os.path.dirname(__file__),
            'queries',
            'client_last_closed_event.sql'
        )
        with open(client_last_closed_event_query_path, 'r') as sql_file:
            self.client_last_closed_event_query_template = sql_file.read()
        
        client_last_reactivation_event_query_path= os.path.join(
            os.path.dirname(__file__),
            'queries',
            'client_last_closed_event.sql'
        )
        with open(client_last_reactivation_event_query_path, 'r') as sql_file:
            self.client_last_reactivation_event_query_template = sql_file.read()

        record_existance_query_path = os.path.join(
            os.path.dirname(__file__),
            'queries',
            'reactivation_event_already_exists.sql'
        )
        with open(record_existance_query_path, 'r') as sql_file:
            self.record_existance_query_template = sql_file.read()


def define_rows_to_append(df_search_reactivation_events: DataFrame, bq_client: Client, **context):
    
    queries_manager= DAGQueriesManager()

    append_rows= []

    for row in df_search_reactivation_events.to_dict('records'):
        
        # Creo la query para ir a buscar el último evento de cierre del cliente 
        # cuyo timestamp sea previo al evento de reapertura que está siendo iterado.
        # Ejecuto la query para ir a buscar ese último evento de cierre 
        # y me traigo una lista de resultados
        client_last_closed_event_query = queries_manager.client_last_closed_event_query_template.format(
            client_id= row.get('client_id'), 
            created_at= row.get('created_at')
        )
        last_closed_client_event_record= bq_client.query(
            query= client_last_closed_event_query
        ).result().to_dataframe().to_dict('records')
        
        # Si no hay eventos de resultados, paso al siguiente evento de busqueda
        if not len(last_closed_client_event_record):
            continue

        # Si hay evento me quedo con el evento y no con la lista
        last_closed_client_event_record= last_closed_client_event_record.pop()

        # Creo la query para ir a buscar el último evento de reactivacion del cliente 
        # cuyo timestamp sea previo al evento de reapertura que está siendo iterado.
        # Ejecuto la query para ir a buscar ese último evento de reactivacion 
        # y me traigo una lista de resultados.
        client_last_reactivation_event_query = queries_manager.client_last_reactivation_event_query_template.format(
            client_id= row.get('client_id'),
            created_at= row.get('created_at')
        )
        last_client_last_reactivation_event_record = bq_client.query(
            query= client_last_reactivation_event_query
        ).result().to_dataframe().to_dict('records')

        # La primera opcion para appendear es si no hay un evento de reactivación previo.
        #La segunda opcion es que si existe pero que el evento que está siendo iterado
        # es posterior al ultimo evento de reactivacion.
        to_append_option_1 = not len(last_client_last_reactivation_event_record)
        to_append_option_2 = (
                len(last_client_last_reactivation_event_record)
            and last_closed_client_event_record.get('created_at') >= \
                last_client_last_reactivation_event_record[0].get('created_at') 
        )
        
        # La primera condicion necesaria es que se cumpla alguna de las opciones anteriores
        record_can_be_appended = (to_append_option_1 or to_append_option_2)

        # La segunda condición es que no haya agregado
        # ya el evento a la tabla de reactivaciones
        check_record_exists_query= queries_manager.record_existance_query_template.format(
            project_id= context['params'].get('project_id'), 
            dataset_id= context['params'].get('mudata_raw'), 
            table_id= 'client_reactivation_events',
            field_name= 'event_id',
            field_value= row.get('event_id')
        )
        record_doesnt_exists = bq_client.query(
            query= check_record_exists_query
        ).result().total_rows == 0

        if record_can_be_appended and record_doesnt_exists:
            append_rows.append(row)

    return append_rows

def validate_search_reactivation_as_client_reactivation(**context):
    bq_client= Client(project= context['params'].get('project_id'), location= 'us-central1')
    
    job_id= context['task_instance'].xcom_pull('query_daily_search_reactivation_events')
    bq_job= bq_client.get_job(job_id= job_id)
    df_search_reactivation_events= bq_job.to_dataframe()
    
    append_rows = define_rows_to_append(df_search_reactivation_events, bq_client, **context)

    if len(append_rows):
        bq_client.load_table_from_dataframe(
            dataframe= DataFrame(data= append_rows), 
            destination= f"{context['params'].get('project_id')}.{context['params'].get('mudata_raw')}.client_reactivation_events",
            job_config= LoadJobConfig(
                create_disposition= CreateDisposition.CREATE_NEVER,
                write_disposition= WriteDisposition.WRITE_APPEND
            )
        )