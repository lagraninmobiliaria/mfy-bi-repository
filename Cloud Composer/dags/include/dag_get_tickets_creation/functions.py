import os

class DAGQueriesManager:

    def __init__(self):

        get_tickets_creation_query_template_path= os.path.join(
            os.path.dirname(__file__),
            'queries',
            'get_tickets_creation_template.sql'
        )
        with open(get_tickets_creation_query_template_path, 'r') as get_tickets_creation_query_template_file:
            self.get_tickets_creation_query_template= get_tickets_creation_query_template_file.read()
