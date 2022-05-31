SELECT
    time_range_questionevents.event_id                  event_id,
    time_range_questionevents.created_at                created_at,
    time_range_questionevents.client_id                 client_id,
    time_range_questionevents.opportunity_id            opportunity_id

FROM (
    SELECT  
        questionevents.event_id                         event_id,
        questionevents.created_at                       created_at,
        questionevents.client_id                        client_id,
        questionevents.opportunity_id                   opportunity_id,
        ROW_NUMBER() OVER(
            PARTITION BY questionevents.client_id 
            ORDER BY questionevents.created_at ASC
        )                                               created_order_n

    FROM `infrastructure-lgi.{{DATASET_MUDATA_RAW}}.questionevents` questionevents
    WHERE  
        NOT EXISTS(
            SELECT 
                *
            FROM `infrastructure-lgi.{{DATASET_MUDATA_CURATED}}.clients_first_questionevent` clients_first_questionevent
            WHERE 
                questionevents.client_id = clients_first_questionevent.client_id
        )
        AND questionevents.created_at >= TIMESTAMP('{{ data_interval_start }}')
        AND questionevents.created_at <  TIMESTAMP('{{ data_interval_end }}')
) time_range_questionevents

WHERE 
    time_range_questionevents.created_order_n = 1