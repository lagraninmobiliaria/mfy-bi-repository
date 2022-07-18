SELECT
development_id  AS development_id,
title           AS name,
street,
street_number,
address AS address,
latitude,
longitude,
location_id,
status AS development_status,
broker_agent_id AS alliance_id,
developer_id,
DATETIME(TIMESTAMP(last_modified_at)) AS update_datetime_z,
summary.min_price as min_listed_price,
currency AS min_listed_currency,
summary.min_price AS max_listed_price,
currency AS max_listed_currency,
summary as development_additionals

FROM `{project_id}.{dataset_id}.developments` developments

