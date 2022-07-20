import os

class DAGQueriesManager:
    def __init__(self) -> None:
        
        get_buying_opportunity_cases_query_path= os.path.join(
            os.path.dirname(__file__),
            'queries',
            'get_buying_opportunity_cases.sql'
        )
        with open(get_buying_opportunity_cases_query_path, 'r') as sql_file:
            self.get_buying_opportunity_cases_query_template= sql_file.read()