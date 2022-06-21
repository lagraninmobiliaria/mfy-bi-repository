DELETE 
FROM `{{ params.project_id }}.{{ params.mudata_raw }}.properties` properties
WHERE properties.created_at IS NULL;