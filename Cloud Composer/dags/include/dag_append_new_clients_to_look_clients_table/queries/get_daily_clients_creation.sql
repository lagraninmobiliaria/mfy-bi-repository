SELECT
    *
FROM `{{ params.project_id }}.{{ params.mudata_raw }}.clients_creation` clients_creation
WHERE
        clients_creation.registered_datetime_z >= TIMESTAMP('{{ data_interval_start }}')
    AND clients_creation.registered_datetime_z <  TIMESTAMP('{{ data_interval_end }}')
