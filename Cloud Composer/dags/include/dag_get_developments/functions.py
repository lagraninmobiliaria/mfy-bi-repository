import os

from pandas import DataFrame

from dependencies.keys_and_constants import schemaTypes

def build_query_to_get_developments(schema_fields, **context):
    
    if schema_fields is None:
        raise ValueError('No schema passed')
    
    table_schema= DataFrame(data= schema_fields)
    table_schema.columns= [col.upper() for col in table_schema.columns]
    columns_for_select= []

    for index, row in table_schema.iterrows():

        select_row= get_select_row(row= row)

        columns_for_select\
            .append(select_row)
            
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

def get_select_row(row):

    conflicted_columns= ['latitude', 'longitude']

    if row.NAME in conflicted_columns:
        select_row= f"ST_Y (ST_Transform (coordinates, 4326)) AS {row.NAME}" \
            if row.name=='latitude'\
            else f"ST_X (ST_Transform (coordinates, 4326)) AS {row.NAME}"
    elif row.NAME == 'development_id':
        select_row= f"id AS {row.NAME}"
    else:
        select_row= f"{row.NAME} AS {row.NAME}"
    
    return select_row