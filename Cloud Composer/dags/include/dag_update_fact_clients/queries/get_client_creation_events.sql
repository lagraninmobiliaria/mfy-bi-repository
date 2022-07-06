SELECT
    clients_creation.*,
    'create_client_event' kind

FROM `{{ params.project_id }}.{{ params.mudata_raw }}.clients_creation` clients_creation
WHERE
        clients_creation.creation_datetime_z >= DATETIME(TIMESTAMP("{{ data_interval_start }}"))
    AND clients_creation.creation_datetime_z <  DATETIME(TIMESTAMP("{{ data_interval_end }}"))
