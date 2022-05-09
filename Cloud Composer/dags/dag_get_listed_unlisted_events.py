from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from datetime import datetime

from dependencies.keys_and_constants import PROJECT_ID, DATASET_MUDATA_CURATED
