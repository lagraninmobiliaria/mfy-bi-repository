SELECT
    *
FROM `{{ params.project_id }}.{{ params.mudata_raw }}.clients_first_question_events` clients_first_question_events
WHERE DATE(clients_first_question_events.created_at) = "{{ data_interval_start.date() }}"