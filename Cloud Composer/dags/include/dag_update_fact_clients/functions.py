import os

class DAGQueriesManager:

    def __init__(
        self, 
        clients_creation_events_file= 'get_client_creation_events.sql',
        client_reactivation_events_file= 'get_client_reactivation_events.sql',
        closed_client_events_file= 'get_closed_client_events.sql',
    ) -> None:
        
        clients_creation_query_path= os.path.join(
            os.path.dirname(__file__),
            'queries',
            clients_creation_events_file
        )
        with open(clients_creation_query_path, 'r') as sql_file:
            self.clients_creation_events_query= sql_file.read()

        client_reactivation_events_query_path= os.path.join(
            os.path.dirname(__file__),
            'queries',
            client_reactivation_events_file
        )
        with open(client_reactivation_events_query_path, 'r') as sql_file:
            self.client_reactivation_events_query= sql_file.read()

        closed_client_events_query_path= os.path.join(
            os.path.dirname(__file__),
            'queries',
            closed_client_events_file
        )
        with open(closed_client_events_query_path, 'r') as sql_file:
            self.closed_client_events_query= sql_file.read()