from dependencies.keys_and_constants    import schemaTypes
from google.cloud.bigquery.table        import TimePartitioningType

LOOK_CLIENTS = {
    "table_id": "look_clients",
    "shema_fields": [
        {"name": "client_id", "type": schemaTypes.INTEGER},
        {"name": "name", "type": schemaTypes.STRING},
        {"name": "creation_date", "type": schemaTypes.DATE},
        {"name": "phone", "type": schemaTypes.STRING},
        {"name": "email", "type": schemaTypes.STRING},
        {"name": "source", "type": schemaTypes.STRING},
        {"name": "country", "type": schemaTypes.STRING},
        {"name": "is_mail_subscription", "type": schemaTypes.BOOL},
        {"name": "marketing_campaign", "type": schemaTypes.STRING},
        {"name": "adgroup", "type": schemaTypes.STRING},
        {"name": "medium", "type": schemaTypes.STRING}
    ],
    "time_partitioning": {
        "field": "creation_date",
        "type": TimePartitioningType.DAY
    }
}

LOOK_PROPERTIES = {
    "table_id": "look_properties",
    "schema_fields": [
        {"name":"property_id", "type": schemaTypes.INTEGER},
        {"name":"property_unique_id", "type": schemaTypes.INTEGER},
        {"name":"external_source_id", "type": schemaTypes.INTEGER},
        {"name":"street", "type": schemaTypes.STRING},
        {"name":"street_number", "type": schemaTypes.INTEGER},
        {"name":"address", "type": schemaTypes.STRING},
        {"name":"latitude", "type": schemaTypes.FLOAT},
        {"name":"longitude", "type": schemaTypes.FLOAT},
        {"name":"location_id", "type": schemaTypes.INTEGER},
        {"name":"country_id", "type": schemaTypes.INTEGER},
        {"name":"title", "type": schemaTypes.STRING},
        {"name":"source", "type": schemaTypes.STRING},
        {"name":"kind", "type": schemaTypes.STRING},
        {"name":"listed_price", "type": schemaTypes.INTEGER},
        {"name":"appraised_price", "type": schemaTypes.INTEGER},
        {"name":"currency", "type": schemaTypes.STRING},
        {"name":"operation_kind", "type": schemaTypes.STRING},
        {"name":"operation_status", "type": schemaTypes.STRING},
        {"name":"link", "type": schemaTypes.STRING},
        {"name":"is_active", "type": schemaTypes.BOOL},
        {"name":"listed_date", "type": schemaTypes.DATE},
        {"name":"unlisted_date", "type": schemaTypes.DATE},
        {"name":"comercial_advisor_id", "type": schemaTypes.INTEGER},
        {"name":"producer_advisor_id", "type": schemaTypes.INTEGER},
        {"name":"client_id", "type": schemaTypes.INTEGER},
        {"name":"alliance_id", "type": schemaTypes.INTEGER},
        {"name":"development_id", "type": schemaTypes.INTEGER},
    ]
}


FACT_PROPERTIES = {
    "table_id": "fact_properties",
    "schema_fields": [
        {"name": "registered_date", "type": schemaTypes.DATETIME},        
        {"name": "property_id", "type": schemaTypes.INTEGER},
        {"name": "price", "type": schemaTypes.INTEGER},
        {"name": "is_active", "type": schemaTypes.BOOL},
        {"name": "currency", "type": schemaTypes.STRING},
        {"name": "comercial_advisor_id", "type": schemaTypes.INTEGER},
        {"name": "producer_advisor_id", "type": schemaTypes.INTEGER},
        {"name": "listed_date", "type": schemaTypes.DATE},
        {"name": "unlisted_date", "type": schemaTypes.DATE},
    ],
    "time_partitioning": {
        "field": "registered_date",
        "type": TimePartitioningType.MONTH
    }
}