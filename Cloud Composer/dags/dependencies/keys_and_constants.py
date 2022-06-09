import os

from sqlalchemy import FLOAT

PROJECT_ID = os.getenv('_PROJECT_ID')
DATASET_MUDATA_RAW = os.getenv('_DATASET_MUDATA_RAW')
DATASET_MUDATA_CURATED = os.getenv('_DATASET_MUDATA_CURATED')
DATASET_MUDATA_AGGREGATED = os.getenv('_DATASET_MUDATA_AGGREGATED')
EXTERNAL_DATA_BUCKET = os.getenv('_EXTERNAL_DATA_BUCKET')

class BUSINESS_ROLES:
    TCC= 'TCC'
    TCP= 'TCP'
    BO= 'BO'
    AP= 'AP'
    AC= 'AC'
    AL= 'AL'

class writeDisposition:
    WRITE_TRUNCATE = "WRITE_TRUNCATE"
    WRITE_APPEND = "WRITE_APPEND"
    WRITE_EMPTY = "WRITE_EMPTY"

    def constants_documentation(option: str) -> str:
        dict_doc = {
            "WRITE_TRUNCATE": "If the table already exists, BigQuery overwrites the table data and uses the schema from the query result.",
            "WRITE_APPEND": "If the table already exists, BigQuery appends the data to the table.",
            "WRITE_EMPTY": "If the table already exists and contains data, a 'duplicate' error is returned in the job result."
        }

        return dict_doc.get(option, f"{option} is not an option")
    
class createDisposition:
    CREATE_IF_NEEDED = "CREATE_IF_NEEDED"
    CREATE_NEVER = "CREATE_NEVER"

    def constants_documentation(option: str) -> str:
        dict_doc = {
            "CREATE_IF_NEEDED": "If the table does not exist, BigQuery creates the table.", 
            "CREATE_NEVER": "The table must already exist. If it does not, a 'notFound' error is returned in the job result."
        }

        return dict_doc.get(option, f"{option} is not an option")

class schemaTypes:
    BOOL= "BOOL"
    STRING= "STRING"
    DATE= "DATE"
    DATETIME= "DATETIME"
    TIMESTAMP= "TIMESTAMP"
    STRUCT= "STRUCT"
    GEOGRAPHY= "GEOGRAPHY"
    INTEGER= "INTEGER"
    FLOAT= "FLOAT64"