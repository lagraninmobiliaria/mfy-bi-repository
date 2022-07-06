import os

from datetime import timedelta

PROJECT_ID = os.getenv('_PROJECT_ID')
DATASET_MUDATA_RAW = os.getenv('_DATASET_MUDATA_RAW')
DATASET_MUDATA_CURATED = os.getenv('_DATASET_MUDATA_CURATED')
DATASET_MUDATA_AGGREGATED = os.getenv('_DATASET_MUDATA_AGGREGATED')

STG_DATASET_MUDATA_RAW = os.getenv('_STG_DATASET_MUDATA_RAW')
STG_DATASET_MUDATA_CURATED = os.getenv('_STG_DATASET_MUDATA_CURATED')
STG_DATASET_MUDATA_AGGREGATED = os.getenv('_STG_DATASET_MUDATA_AGGREGATED')

PROD_DATASET_MUDATA_RAW = os.getenv('_PROD_DATASET_MUDATA_RAW')
PROD_DATASET_MUDATA_CURATED = os.getenv('_PROD_DATASET_MUDATA_CURATED')
PROD_DATASET_MUDATA_AGGREGATED = os.getenv('_PROD_DATASET_MUDATA_AGGREGATED')

class BUSINESS_ROLES:
    TCC= 'TCC'
    TCP= 'TCP'
    BO= 'BO'
    AP= 'AP'
    AC= 'AC'
    AL= 'AL'

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

std_default_args_dag = dict(
    is_paused_upon_creation= True,
    owner= 'airflow',
    depends_on_past= False,
    email_on_failure= False,
    email_on_retry= False,
    retries= 1,
    retry_delay= timedelta(minutes= 1)
)

STG_PARAMS= {
    "env_prefix": "stg",
    "project_id": PROJECT_ID,
    "mudata_raw": STG_DATASET_MUDATA_RAW,
    "mudata_curated": STG_DATASET_MUDATA_CURATED,
    "mudata_aggregated": STG_DATASET_MUDATA_AGGREGATED
}


PROD_PARAMS= {
    "env_prefix": "prod",
    "project_id": PROJECT_ID,
    "mudata_raw": PROD_DATASET_MUDATA_RAW,
    "mudata_curated": PROD_DATASET_MUDATA_CURATED,
    "mudata_aggregated": PROD_DATASET_MUDATA_AGGREGATED
}