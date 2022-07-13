import os

from pandas import DataFrame


def build_query_to_get_developments(schema_fields, **context):
    
    if schema_fields is None:
        raise ValueError('No schema passed')
    
    table_schema= DataFrame(data= schema_fields)
    columns_for_select= []

    for index, row in table_schema.iterrows():

        if row['name'] == 'latitude':
            table_schema.loc[index, 'name_for_query']= "ST_Y (ST_Transform (coordinates, 4326))"
        elif row['name'] == 'longitude':
            table_schema.loc[index, 'name_for_query']= "ST_X (ST_Transform (coordinates, 4326))"
        else:
            table_schema.loc[index, 'name_for_query']= row['name']

        columns_for_select\
            .append(f"{ table_schema.loc[index, 'name_for_query']  if row['name'] != 'development_id' else 'id'} AS {row['name']}")

    string_for_select= ',\n'.join(columns_for_select)
    get_developments_query_path= os.path.join(
        os.path.dirname(__file__),
        'queries',
        'get_developments.sql'
    )
    with open(get_developments_query_path, 'r') as sql_file:
        get_developments_query= sql_file\
            .read()\
            .format(string_for_select= string_for_select)
    
    return get_developments_query
