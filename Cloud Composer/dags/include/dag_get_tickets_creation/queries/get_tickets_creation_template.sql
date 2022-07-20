SELECT
    tt.created_at   AS registered_datetime_z,
    tt.id           AS ticket_id,
    tt.client_id    AS client_id,
    tt.country      AS country_code_iso2,
    tt.owner_id     AS user_id,
    tt.created_at   AS tt.created_datetime_z

FROM tickets_ticket tt
WHERE
    DATE(tt.created_at) = DATE({{ data_interval_start.date() }})