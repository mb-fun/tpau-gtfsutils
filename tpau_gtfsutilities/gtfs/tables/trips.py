from .gtfstable import GTFSTable
from .helpers import ColumnRef

class Trips(GTFSTable):
    index=['trip_id']
    downstream_columns = {
        'trip_id': {
            'references': [
                ColumnRef('stop_times', 'trip_id'),
                ColumnRef('frequencies', 'trip_id'),
                ColumnRef('attributions', 'trip_id')
            ]
        }
    }
    upstream_columns = {
        'service_id': {
            'references': [ColumnRef('calendar', 'service_id'), ColumnRef('calendar_dates', 'service_id')]
        },
        'route_id': {
            'references': [ColumnRef('routes', 'route_id')]
        },
        'shape_id': {
            'references': [ColumnRef('shapes', 'shape_id')]
        }
    }

