SELECT  
    MIN(questionevents.event_id)        event_id,
    MIN(questionevents.created_at)      created_at,
    questionevents.client_id            client_id,
    MIN(questionevents.opportunity_id)  opportunity_id
FROM `infrastructure-lgi.{{DATASET_MUDATA_RAW}}.questionevents` questionevents
WHERE  
    NOT EXISTS(
        SELECT client_id
        FROM `infrastructure-lgi.{{DATASET_MUDATA_CURATED}}.clients_first_questionevent` clients_first_questionevent
        WHERE questionevents.client_id = clients_first_questionevent.client_id
    )
    AND questionevents.created_at >= TO_TIMESTAMP('{{ data_interval_start }}', 'YYYY-MM-DDTHH24:MI:SS')
    AND questionevents.created_at <  TO_TIMESTAMP('{{ data_interval_end }}', 'YYYY-MM-DDTHH24:MI:SS')

GROUP BY
    questionevents.client_id
