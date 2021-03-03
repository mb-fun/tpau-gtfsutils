from .gtfstable import GTFSTable
from .helpers import ColumnRef

class StopTimes(GTFSTable):
    upstream_columns = {
        'trip_id': {
            'references': [ColumnRef('trips', 'trip_id')]
        },
        'stop_id': {
            'references': [ColumnRef('stops', 'stop_id')]
        }
    }
