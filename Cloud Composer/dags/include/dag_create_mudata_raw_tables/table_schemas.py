from dependencies.keys_and_constants import schemaTypes
from google.cloud.bigquery.table import TimePartitioningType

CLIENT_CREATION = {
    "table_id": "client_creation",
    "schema_fields": [
        {"name": "registered_date", "type": schemaTypes.DATETIME},
        {"name": "client_id", "type": schemaTypes.INTEGER},
        {"name": "name", "type": schemaTypes.STRING},
        {"name": "phone","type": schemaTypes.STRING},
        {"name": "email","type": schemaTypes.STRING},
        {"name": "source","type": schemaTypes.STRING},
        {"name": "country","type": schemaTypes.STRING},
        {"name": "creation_date","type": schemaTypes.DATETIME},
    ],
    "time_partitioning": {
        "field": "registered_date", 
        "type": TimePartitioningType.DAY 
    },
}

CLIENT_MAIL_SUBSCRIPTION = {
    "table_id": "client_mail_subscription",
    "schema_fields": [
        {"name": "registered_date", "type": schemaTypes.DATETIME},
        {"name": "client_id", "type": schemaTypes.INTEGER},
        {"name": "subscription_date", "type": schemaTypes.DATETIME},
        {"name": "condition", "type": schemaTypes.STRING}
    ],
    "time_partitioning": {
        "field": "registered_date", 
        "type": TimePartitioningType.DAY 
    },
}

BUYING_OPPORTUNITY_CASE = {
    "table_id": "buying_opportunity_case",
    "schema_fields": [
        {"name": "registered_date", "type": schemaTypes.DATETIME},
        {"name": "event_id", "type": schemaTypes.INTEGER},
        {"name": "opportunity_id", "type": schemaTypes.INTEGER},
        {"name": "client_id", "type": schemaTypes.INTEGER},
        {"name": "ticket_id", "type": schemaTypes.INTEGER},
        {"name": "property_id", "type": schemaTypes.INTEGER},
        {"name": "develompent_id", "type": schemaTypes.INTEGER},
        {"name": "source", "type": schemaTypes.STRING},
        {"name": "reference", "type": schemaTypes.STRING},
        {"name": "is_new", "type": schemaTypes.BOOL}
    ],
    "time_partitioning": {
        "field": "registered_date", 
        "type": TimePartitioningType.DAY 
    },
}

TASKS = {
    "table_id": "tasks",
    "schema_fields": [
        {"name": "registered_date", "type": schemaTypes.DATETIME},
        {"name":"task_id", "type": schemaTypes.INTEGER},
        {"name":"creation_date", "type": schemaTypes.DATETIME},
        {"name":"deadline_date", "type": schemaTypes.DATETIME},
        {"name":"completion_date", "type": schemaTypes.DATETIME},
        {"name":"title", "type": schemaTypes.STRING},
        {"name":"description", "type": schemaTypes.STRING},
        {"name":"assignee_id", "type": schemaTypes.INTEGER},
        {"name":"client_id", "type":schemaTypes.INTEGER},
        {"name":"property_id", "type":schemaTypes.INTEGER},
        {"name":"kind", "type": schemaTypes.STRING},
        {"name":"completed_by_id", "type":schemaTypes.INTEGER},
        {"name":"created_by_id", "type":schemaTypes.INTEGER},
        {"name":"deadline_type", "type": schemaTypes.STRING},
        {"name":"develompent_id", "type": schemaTypes.INTEGER},
        {"name":"opportunity_id", "type": schemaTypes.INTEGER},
        {"name":"is_completed", "type": schemaTypes.BOOL},
        {"name":"status", "type": schemaTypes.STRING},
        {"name":"process_step_id", "type": schemaTypes.INTEGER},
        {"name":"last_modified_at", "type": schemaTypes.DATETIME},
        {"name":"next_priority_check", "type": schemaTypes.DATETIME},
        {"name":"sla", "type": schemaTypes.DATETIME},
        {"name":"booking_id", "type": schemaTypes.INTEGER}
    ],
    "time_partitioning": {
        "field": "registered_date", 
        "type": TimePartitioningType.DAY 
    },
}

USER_ASSIGNMENT = {
    "table_id": "user_assignment",
    "schema_fields": [
        {"name": "registered_date", "type": schemaTypes.DATETIME},
        {"name": "event_id", "type": schemaTypes.INTEGER},
        {"name": "client_id", "type": schemaTypes.INTEGER},
        {"name": "ticket_id", "type": schemaTypes.INTEGER},
        {"name": "new_owner_id", "type": schemaTypes.INTEGER},
        {"name": "old_owner_id", "type": schemaTypes.INTEGER},
        {"name": "from_datetime_z", "type": schemaTypes.DATETIME},
        {"name": "to_datetime_z", "type": schemaTypes.DATETIME},
    ],
    "time_partitioning": {
        "field": "registered_date", 
        "type_": TimePartitioningType.DAY 
    },
}

TICKETS = {
    "table_id": "tickets",
    "schema_fields": [
        {"name": "registered_date", "type": schemaTypes.DATETIME},
        {"name": "ticket_id", "type": schemaTypes.INTEGER},
        {"name": "client_id", "type": schemaTypes.INTEGER},
        {"name": "country_id", "type": schemaTypes.INTEGER},
        {"name": "opportunity_id", "type": schemaTypes.INTEGER},
        {"name": "user_id", "type": schemaTypes.INTEGER},
        {"name": "created_datetime_z", "type": schemaTypes.DATETIME},
        {"name": "last_update_datetime_z", "type": schemaTypes.DATETIME},
        {"name": "last_update_by", "type": schemaTypes.INTEGER},
        {"name": "status", "type": schemaTypes.STRING},
        {"name": "is_new", "type": schemaTypes.BOOL},
    ],
    "time_partitioning": {
        "field": "registered_date", 
        "type": TimePartitioningType.DAY 
    },
}