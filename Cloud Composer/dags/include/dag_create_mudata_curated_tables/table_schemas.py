from dependencies.keys_and_constants    import schemaTypes
from google.cloud.bigquery.table        import TimePartitioningType

LOOK_CLIENTS = {
    "table_id": "look_clients",
    "schema_fields": [
        {"name": "client_id", "type": schemaTypes.INTEGER},
        {"name": "name", "type": schemaTypes.STRING},
        {"name": "creation_datetime_z", "type": schemaTypes.DATETIME},
        {"name": "phone", "type": schemaTypes.STRING},
        {"name": "email", "type": schemaTypes.STRING},
        {"name": "country", "type": schemaTypes.STRING},
        {"name": "is_mail_subscription", "type": schemaTypes.BOOL},
    ],
    "time_partitioning": {
        "field": "creation_datetime_z",
        "type": TimePartitioningType.DAY
    },
    "cluster_fields": ['client_id', 'country', 'email']
}

FACT_CLIENTS = {
    "table_id": "fact_clients",
    "schema_fields": [
        {"name": "client_id", "type": schemaTypes.INTEGER, "description": "Client identifier on Mudafy"},   
        {"name": "from_datetime_z", "type": schemaTypes.DATETIME, "description": "Datetime since the user is active"},
        {"name": "to_datetime_z", "type": schemaTypes.DATETIME, "description": "Datetime until the user is active"},
        {"name": "is_active", "type": schemaTypes.BOOL, "description": "Defines if the period during 'from_datetime_z' - 'to_datetime_z' period is a client activation"},
        {"name": "is_reactive", "type": schemaTypes.BOOL, "description": "Defines if the period during 'from_datetime_z' - 'to_datetime_z' period is a client reactivation"},
        {"name": "last_modified_datetime_z", "type": schemaTypes.DATETIME, "description": "Last event modification datetime"},
    ],
    "time_partitioning": {
        "field": "from_datetime_z",
        "type": TimePartitioningType.DAY
    },
    "cluster_fields": ['client_id']
}

LOOK_USERS = {
    "table_id": "look_users",
    "schema_fields": [
        {"name": "user_key", "type": schemaTypes.INTEGER},
        {"name": "user_id", "type": schemaTypes.INTEGER},
        {"name": "name", "type": schemaTypes.STRING},
        {"name": "role", "type": schemaTypes.STRING},
        {"name": "team", "type": schemaTypes.STRING},
        {"name": "leader_id", "type": schemaTypes.INTEGER},
        {"name": "region", "type": schemaTypes.STRING},
        {"name": "country", "type": schemaTypes.STRING},
        {"name": "mail", "type": schemaTypes.STRING},
        {"name": "phone", "type": schemaTypes.STRING},
        {"name": "registration_date", "type": schemaTypes.DATE},
        {"name": "is_user_active", "type": schemaTypes.BOOL},
    ],
    "time_partitioning": dict(
        field= "registration_date",
        type= TimePartitioningType.MONTH
    )
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
        {"name":"commercial_advisor_id", "type": schemaTypes.INTEGER},
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

LOOK_PROPERTIES_ADDITIONAL = {
    "table_id": "look_properties_additional",
    "schema_fields": [
        {"name": "property_id", "type": schemaTypes.INTEGER},
        {"name": "total_area", "type": schemaTypes.FLOAT},
        {"name": "covered_area", "type": schemaTypes.FLOAT},
        {"name": "semi_covered_area", "type": schemaTypes.FLOAT},
        {"name": "uncovered_area", "type": schemaTypes.FLOAT},
        {"name": "number_room", "type": schemaTypes.INTEGER},
        {"name": "number_bedroom", "type": schemaTypes.INTEGER},
        {"name": "number_toilet", "type": schemaTypes.INTEGER},
        {"name": "number_bath", "type": schemaTypes.INTEGER},
        {"name": "number_garage", "type": schemaTypes.INTEGER},
        {"name": "tour_link", "type": schemaTypes.STRING},
        {"name": "update_date", "type": schemaTypes.DATE},
    ],
    "time_partitioning": dict(
        field= "update_date",
        type= TimePartitioningType.MONTH
    )
}

LOOK_ALLIANCES = {
    "table_id": "look_alliances",
    "schema_fields": [
        {"name": "alliance_id", "type": schemaTypes.INTEGER},
        {"name": "name", "type": schemaTypes.STRING},
        {"name": "name_broker", "type": schemaTypes.STRING},
        {"name": "source", "type": schemaTypes.STRING},
        {"name": "country", "type": schemaTypes.STRING},
        {"name": "is_alliance_active", "type": schemaTypes.BOOL},
        {"name": "reason", "type": schemaTypes.STRING},    
    ]
}

LOOK_DEVELOPMENTS = {
    "table_id": "look_developments",
    "schema_fields": [
        {"name": "development_id", "type": schemaTypes.INTEGER},
        {"name": "name", "type": schemaTypes.STRING},
        {"name": "street", "type": schemaTypes.STRING},
        {"name": "street_number", "type": schemaTypes.INTEGER},
        {"name": "address", "type": schemaTypes.STRING},
        {"name": "latitude", "type": schemaTypes.FLOAT},
        {"name": "longitude", "type": schemaTypes.FLOAT},
        {"name": "location_id", "type": schemaTypes.INTEGER},
        {"name": "development_status", "type": schemaTypes.STRING},
        {"name": "alliance_id", "type": schemaTypes.INTEGER},
        {"name": "developer_id", "type": schemaTypes.INTEGER},
        {"name": "update_date", "type": schemaTypes.DATE},
        {"name": "min_listed_price", "type": schemaTypes.INTEGER},
        {"name": "min_listed_currency", "type": schemaTypes.STRING},
        {"name": "min_number_rooms", "type": schemaTypes.INTEGER},
        {"name": "min_total_area", "type": schemaTypes.FLOAT},
        {"name": "min_covered_area", "type": schemaTypes.FLOAT},
        {"name": "max_listed_price", "type": schemaTypes.INTEGER},
        {"name": "max_listed_currency", "type": schemaTypes.INTEGER},
        {"name": "max_number_room", "type": schemaTypes.INTEGER},
        {"name": "max_total_area", "type": schemaTypes.FLOAT},
        {"name": "max_covered_area", "type": schemaTypes.FLOAT},
    ]
}