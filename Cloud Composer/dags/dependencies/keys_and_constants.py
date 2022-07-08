import os

from datetime import timedelta

from google.cloud.bigquery import Client

bq_client= Client()

PROJECT_ID = os.getenv('_PROJECT_ID')
DATASET_MUDATA_RAW = os.getenv('_DATASET_MUDATA_RAW')
DATASET_MUDATA_CURATED = os.getenv('_DATASET_MUDATA_CURATED')
DATASET_MUDATA_AGGREGATED = os.getenv('_DATASET_MUDATA_AGGREGATED')

STG_DATASET_MUDATA_RAW = os.getenv('_STG_DATASET_MUDATA_RAW')
STG_DATASET_MUDATA_CURATED = os.getenv('_STG_DATASET_MUDATA_CURATED')
STG_DATASET_MUDATA_AGGREGATED = os.getenv('_STG_DATASET_MUDATA_AGGREGATED')

STG_DATASET_MUDATA_RAW_TABLES= bq_client.list_tables(dataset= STG_DATASET_MUDATA_RAW)
STG_DATASET_MUDATA_CURATED_TABLES= bq_client.list_tables(dataset= STG_DATASET_MUDATA_CURATED)
STG_DATASET_MUDATA_AGGREGATED_TABLES= bq_client.list_tables(dataset= STG_DATASET_MUDATA_AGGREGATED)

PROD_DATASET_MUDATA_RAW = os.getenv('_PROD_DATASET_MUDATA_RAW')
PROD_DATASET_MUDATA_CURATED = os.getenv('_PROD_DATASET_MUDATA_CURATED')
PROD_DATASET_MUDATA_AGGREGATED = os.getenv('_PROD_DATASET_MUDATA_AGGREGATED')

PROD_DATASET_MUDATA_RAW_TABLES= bq_client.list_tables(dataset= PROD_DATASET_MUDATA_RAW)
PROD_DATASET_MUDATA_CURATED_TABLES= bq_client.list_tables(dataset= PROD_DATASET_MUDATA_CURATED)
PROD_DATASET_MUDATA_AGGREGATED_TABLES= bq_client.list_tables(dataset= PROD_DATASET_MUDATA_AGGREGATED)

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

class CLIENT_EVENTS:
    CREATION= "create_client_event"
    REACTIVATION= "reactivate_client_event"
    CLOSURE= "close_client_event"

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