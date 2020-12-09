REQUIRED_TABLES = [ \
    'agency' \
    'stops' \
    'routes' \
    'trips' \
    'stop_times' \
]

TABLE_INDECES = { \
    'stops': ['stop_id'], \
    'routes': ['route_id'], \
    'trips': ['trip_id'], \
    'calendar': ['service_id'], \
    'fare_attributes': ['fare_id'], \
    'shapes': ['shape_id'] \
}

# Fields that should be treated as floats or ints, particularly
# those that we want to use in utilities.
# All other fields are treated as objects
NUMERIC_DTYPES = {
    'stop_lat': 'float',
    'stop_lon': 'float',
    'stop_sequence': 'int',
    'shape_dist_traveled': 'float',
    'shape_pt_sequence': 'int',
    'shape_pt_lat': 'float',
    'shape_pt_lon': 'float',
    'headway_secs': 'int',
}
