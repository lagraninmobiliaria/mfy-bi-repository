SELECT
    * 
FROM EXTERNAL_QUERY(
    "projects/infrastructure-lgi/locations/us/connections/mudafy", 
    """
    WITH v_client_first_questionevent AS (
        SELECT
            ee.client_id        client_id,
            MIN(ee.id)          event_id,
            MIN(ee.created_at)  event_created_at

        FROM events_questionevent eqe
        LEFT JOIN events_event ee
            ON ee.id = eqe.event_ptr_id

        GROUP BY
            ee.client_id

        HAVING 
                MIN(ee.created_at) >= TO_TIMESTAMP('{{ data_interval_start }}', 'YYYY-MM-DDTHH24:MI:SS')
            AND MIN(ee.created_at) <  TO_TIMESTAMP('{{ data_interval_end }}', 'YYYY-MM-DDTHH24:MI:SS')
    )
    SELECT
        v_client_first_questionevent.*                ,
        ee.opportunity_case_id          opportunity_id

    FROM v_client_first_questionevent
        LEFT JOIN events_event ee
            ON ee.id = v_client_first_questionevent.event_id
    """
)