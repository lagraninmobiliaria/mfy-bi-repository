SELECT
    COUNT(*) register_exists
FROM `{project_id}.{dataset_id}.{table_id}` _table
WHERE 
    _table.{field_name} = {field_value}
    
