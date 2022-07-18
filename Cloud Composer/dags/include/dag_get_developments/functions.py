import os

from textwrap import dedent

from pandas import DataFrame

from dependencies.keys_and_constants import schemaTypes

def build_query_to_get_developments(schema_fields, **context):
    
    if schema_fields is None:
        raise ValueError('No schema passed')
    
    table_schema= DataFrame(data= schema_fields)
    table_schema.columns= [col.upper() for col in table_schema.columns]
    columns_for_select= []

    for index, row in table_schema.iterrows():

        if row.NAME == "registered_datetime_z":
            print(row.name)
            continue

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
    elif row.NAME == 'summary':
        select_row= dedent("""
            summary -> 'max_price' AS summary_max_price,
            summary -> 'min_price' AS summary_min_price,
            summary -> 'min_garages' AS summary_min_garages,
            summary -> 'max_garages' AS summary_max_garages,
            summary -> 'min_bedrooms' AS summary_min_bedrooms,
            summary -> 'max_bedrooms' AS summary_max_bedrooms,
            summary -> 'min_bathrooms' AS summary_min_bathrooms,
            summary -> 'max_bathrooms' AS summary_max_bathrooms,
            summary -> 'min_toilettes' AS summary_min_toilettes,
            summary -> 'max_toilettes' AS summary_max_toilettes,
            summary -> 'min_room_count' AS summary_min_room_count,
            summary -> 'max_room_count' AS summary_max_room_count,
            summary -> 'min_total_area' AS summary_min_total_area,
            summary -> 'max_total_area' AS summary_max_total_area,
            summary -> 'min_roofed_area' AS summary_min_roofed_area,
            summary -> 'max_roofed_area' AS summary_max_roofed_area
        """)
    else:
        select_row= f"{row.NAME} AS {row.NAME}"
    
    return select_row