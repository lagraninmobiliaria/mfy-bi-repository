SELECT
    clients_creation.client_id                 AS client_id,              		
    clients_creation.name                      AS name,		            
    clients_creation.creation_datetime_z       AS creation_datetime_z,		
    clients_creation.phone                     AS phone,		            
    clients_creation.email                     AS email,		            
    clients_creation.country                   AS country,		        
    false                                      AS is_mail_subscription  

FROM `{{ params.project_id }}.{{ params.mudata_raw }}.clients_creation` clients_creation
WHERE
        clients_creation.registered_datetime_z >= DATETIME(TIMESTAMP('{{ data_interval_start }}'))
    AND clients_creation.registered_datetime_z <  DATETIME(TIMESTAMP('{{ data_interval_end }}'))
