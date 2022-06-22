SELECT
    time_range_question_events.event_id                  event_id,
    time_range_question_events.created_at                created_at,
    time_range_question_events.client_id                 client_id,
    time_range_question_events.opportunity_id            opportunity_id

FROM (
    SELECT  
        question_events.event_id                         event_id,
        question_events.created_at                       created_at,
        question_events.client_id                        client_id,
        question_events.opportunity_id                   opportunity_id,
        ROW_NUMBER() OVER(
            PARTITION BY question_events.client_id 
            ORDER BY question_events.created_at ASC
        )                                               created_order_n

    FROM `infrastructure-lgi.{{ params.mudata_raw }}.question_events` question_events
    WHERE  
        NOT EXISTS(
            SELECT 
                *
            FROM `infrastructure-lgi.{{ params.mudata_raw }}.clients_first_question_events` clients_first_question_events
            WHERE 
                question_events.client_id = clients_first_question_events.client_id
        )
        AND question_events.created_at >= TIMESTAMP('{{ data_interval_start }}')
        AND question_events.created_at <  TIMESTAMP('{{ data_interval_end }}')
) time_range_question_events

WHERE 
    time_range_question_events.created_order_n = 1