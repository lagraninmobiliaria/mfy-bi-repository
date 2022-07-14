from dependencies.keys_and_constants import schemaTypes
from google.cloud.bigquery.table import TimePartitioningType

CLIENTS_CREATION = {
    "table_id": "clients_creation",
    "schema_fields": [
        {"name": "registered_datetime_z", "type": schemaTypes.DATETIME},
        {"name": "client_id", "type": schemaTypes.INTEGER},
        {"name": "name", "type": schemaTypes.STRING},
        {"name": "email", "type": schemaTypes.STRING},
        {"name": "phone", "type": schemaTypes.STRING},
        {"name": "country_code_iso2", "type": schemaTypes.STRING},
        {"name": "creation_datetime_z", "type": schemaTypes.DATETIME},
    ],
    "time_partitioning": {
        "field": "registered_datetime_z", 
        "type": TimePartitioningType.DAY 
    },
}

CLIENTS_MAIL_SUBSCRIPTION = {
    "table_id": "clients_mail_subscription",
    "schema_fields": [
        {"name": "registered_datetime_z", "type": schemaTypes.DATETIME},
        {"name": "client_id", "type": schemaTypes.INTEGER},
        {"name": "subscription_datetime_z", "type": schemaTypes.DATETIME},
        {"name": "condition", "type": schemaTypes.STRING}
    ],
    "time_partitioning": {
        "field": "registered_datetime_z", 
        "type": TimePartitioningType.DAY 
    },
}

BUYING_OPPORTUNITY_CASES = {
    "table_id": "buying_opportunity_cases",
    "schema_fields": [
        {"name": "registered_datetime_z", "type": schemaTypes.DATETIME},
        {"name": "event_id", "type": schemaTypes.INTEGER},
        {"name": "opportunity_id", "type": schemaTypes.INTEGER},
        {"name": "client_id", "type": schemaTypes.INTEGER},
        {"name": "ticket_id", "type": schemaTypes.INTEGER},
        {"name": "property_id", "type": schemaTypes.INTEGER},
        {"name": "development_id", "type": schemaTypes.INTEGER},
        {"name": "source", "type": schemaTypes.STRING},
        {"name": "kind", "type": schemaTypes.STRING},
        {"name": "previous_kind", "type": schemaTypes.STRING}
    ],
    "time_partitioning": {
        "field": "registered_datetime_z", 
        "type": TimePartitioningType.DAY 
    },
}

BUYING_OPPORTUNITY_REACTIVATION_EVENTS= {
    "table_id": "BUYING_OPPORTUNITY_REACTIVATION_EVENTS".lower(),
    "schema_fields": [
        {"name": "event_id", "type": schemaTypes.INTEGER},
        {"name": "opportunity_id", "type": schemaTypes.INTEGER},
        {"name": "created_datetime_z", "type": schemaTypes.TIMESTAMP},
        {"name": "kind", "type": schemaTypes.STRING},
        {"name": "previous_kind", "type": schemaTypes.STRING},
    ],
    "time_partitioning": {
        "field": "created_datetime_z", 
        "type": TimePartitioningType.DAY 
    },
}

TASKS = {
    "table_id": "tasks",
    "schema_fields": [
        {"name": "registered_datetime_z", "type": schemaTypes.DATETIME},
        {"name":"task_id", "type": schemaTypes.INTEGER},
        {"name":"creation_datetime_z", "type": schemaTypes.DATETIME},
        {"name":"deadline_datetime_z", "type": schemaTypes.DATETIME},
        {"name":"completion_datetime_z", "type": schemaTypes.DATETIME},
        {"name":"title", "type": schemaTypes.STRING},
        {"name":"description", "type": schemaTypes.STRING},
        {"name":"assignee_id", "type": schemaTypes.INTEGER},
        {"name":"client_id", "type":schemaTypes.INTEGER},
        {"name":"property_id", "type":schemaTypes.INTEGER},
        {"name":"kind", "type": schemaTypes.STRING},
        {"name":"completed_by_id", "type":schemaTypes.INTEGER},
        {"name":"created_by_id", "type":schemaTypes.INTEGER},
        {"name":"deadline_type", "type": schemaTypes.STRING},
        {"name":"development_id", "type": schemaTypes.INTEGER},
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
        "field": "registered_datetime_z", 
        "type": TimePartitioningType.DAY 
    },
}

USER_ASSIGNMENTS = {
    "table_id": "user_assignments",
    "schema_fields": [
        {"name": "registered_datetime_z", "type": schemaTypes.DATETIME},
        {"name": "event_id", "type": schemaTypes.INTEGER},
        {"name": "client_id", "type": schemaTypes.INTEGER},
        {"name": "new_owner_id", "type": schemaTypes.INTEGER},
        {"name": "old_owner_id", "type": schemaTypes.INTEGER},
    ],
    "time_partitioning": {
        "field": "registered_datetime_z", 
        "type_": TimePartitioningType.DAY 
    },
}

TICKETS_CREATION = {
    "table_id": "tickets_creation",
    "schema_fields": [
        {"name": "registered_datetime_z", "type": schemaTypes.DATETIME},
        {"name": "ticket_id", "type": schemaTypes.INTEGER},
        {"name": "client_id", "type": schemaTypes.INTEGER},
        {"name": "country_code_iso2", "type": schemaTypes.STRING},
        {"name": "opportunity_id", "type": schemaTypes.INTEGER},
        {"name": "user_id", "type": schemaTypes.INTEGER},
        {"name": "created_datetime_z", "type": schemaTypes.DATETIME},
    ],
    "time_partitioning": {
        "field": "registered_datetime_z", 
        "type": TimePartitioningType.DAY 
    },
}

DEVELOPMENTS= {
    "table_id": "developments",
    "schema_fields": [
        {"name": "development_id", "type": schemaTypes.INTEGER},
        {"name": "last_modified_at", "type": schemaTypes.DATETIME},
        {"name": "slug", "type": schemaTypes.STRING},
        {"name": "title", "type": schemaTypes.STRING},
        {"name": "description", "type": schemaTypes.STRING},
        {"name": "address", "type": schemaTypes.STRING},
        {"name": "street", "type": schemaTypes.STRING},
        {"name": "street_number", "type": schemaTypes.INTEGER},
        {"name": "public_street_number", "type": schemaTypes.INTEGER},
        {"name": "nearby_landmarks", "type": schemaTypes.STRING},
        {"name": "latitude", "type": schemaTypes.FLOAT},
        {"name": "longitude", "type": schemaTypes.FLOAT},
        {"name": "price", "type": schemaTypes.INTEGER},
        {"name": "currency", "type": schemaTypes.STRING},
        {"name": "professional_use", "type": schemaTypes.BOOL},
        {"name": "status", "type": schemaTypes.STRING},
        {"name": "amenities_id", "type": schemaTypes.INTEGER},
        {"name": "building_id", "type": schemaTypes.INTEGER},
        {"name": "developer_id", "type": schemaTypes.INTEGER},
        {"name": "dimensions_id", "type": schemaTypes.INTEGER},
        {"name": "location_id", "type": schemaTypes.INTEGER},
        {"name": "manager_id", "type": schemaTypes.INTEGER},
        {"name": "video_link", "type": schemaTypes.STRING},
        {"name": "due_date", "type": schemaTypes.DATETIME},
        {"name": "show", "type": schemaTypes.BOOL},
        {"name": "country", "type": schemaTypes.STRING},
        {"name": "source", "type": schemaTypes.STRING},
        {"name": "source_id", "type": schemaTypes.INTEGER},
        {"name": "summary", "type": schemaTypes.STRUCT},
        {"name": "tour_link", "type": schemaTypes.STRING},
        {"name": "public_address", "type": schemaTypes.STRING},
        {"name": "finder_score", "type": schemaTypes.INTEGER},
        {"name": "ranking", "type": schemaTypes.INTEGER},
        {"name": "commercial_agent_id", "type": schemaTypes.INTEGER},
        {"name": "ranking_last_modified", "type": schemaTypes.DATETIME},
        {"name": "created_by_id", "type": schemaTypes.INTEGER},
        {"name": "last_modified_by_id", "type": schemaTypes.INTEGER},
        {"name": "created_at", "type": schemaTypes.DATETIME},
        {"name": "forced_changes", "type": schemaTypes.STRUCT},
        {"name": "broker_agent_id", "type": schemaTypes.INTEGER},
        {"name": "last_modified_by_broker", "type": schemaTypes.DATETIME},
        {"name": "notes", "type": schemaTypes.STRING},
        {"name": "google_drive_link", "type": schemaTypes.STRING},
        {"name": "raw_description", "type": schemaTypes.STRING},
    ],
    "time_partitioning": {
        "field": "created_at", 
        "type": TimePartitioningType.DAY 
    },
}