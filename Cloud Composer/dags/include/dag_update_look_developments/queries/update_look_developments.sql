SELECT
development_id                          AS development_id,
title                                   AS name,
street,
street_number,
address                                 AS address,
latitude,
longitude,
location_id,
status                                  AS development_status,
broker_agent_id                         AS alliance_id,
developer_id,
DATETIME(TIMESTAMP(last_modified_at))   AS update_datetime_z,
CASE
    WHEN summary.min_price != 'null' THEN CAST(summary.min_price AS INT64)
    ELSE NULL
END                                     AS min_listed_price,
currency                                AS min_listed_currency,
CASE
    WHEN summary.max_price != 'null' THEN CAST(summary.max_price AS INT64)
    ELSE NULL
END                                     AS max_listed_price,
currency                                AS max_listed_currency,
summary                                 AS development_additionals

FROM `{project_id}.{dataset_id}.developments` developments

