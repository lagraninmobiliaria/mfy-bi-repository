SELECT 
    * EXCEPT(
      summary_max_price,
      summary_min_price,
      summary_min_garages,
      summary_max_garages,
      summary_min_bedrooms,
      summary_max_bedrooms,
      summary_min_bathrooms,
      summary_max_bathrooms,
      summary_min_toilettes,
      summary_max_toilettes,
      summary_min_room_count,
      summary_max_room_count,
      summary_min_total_area,
      summary_max_total_area,
      summary_min_roofed_area,
      summary_max_roofed_area
    ),
    STRUCT(
      summary_max_price AS max_price,
      summary_min_price AS min_price,
      summary_min_garages AS min_garages,
      summary_max_garages AS max_garages,
      summary_min_bedrooms AS min_bedrooms,
      summary_max_bedrooms AS max_bedrooms,
      summary_min_bathrooms AS min_bathrooms,
      summary_max_bathrooms AS max_bathrooms,
      summary_min_toilettes AS min_toilettes,
      summary_max_toilettes AS max_toilettes,
      summary_min_room_count AS min_room_count,
      summary_max_room_count AS max_room_count,
      summary_min_total_area AS min_total_area,
      summary_max_total_area AS max_total_area,
      summary_min_roofed_area AS min_roofed_area,
      summary_max_roofed_area AS max_roofed_area
    ) AS summary
    
FROM EXTERNAL_QUERY(
    "projects/infrastructure-lgi/locations/us-central1/connections/mudafy-prod-replic-us-central",
    """
    SELECT
        {string_for_select}
    FROM properties_development
    """
)