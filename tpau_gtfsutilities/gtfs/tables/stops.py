from .gtfstable import GTFSTable
from .helpers import ColumnRef

class Stops(GTFSTable):
    index=['stop_id']
    downstream_columns = {
        'stop_id': {
            'references': [
                ColumnRef('stops', 'parent_station', cascade_row=False),
                ColumnRef('stop_times', 'stop_id'),
                ColumnRef('transfers', 'from_stop_id'),
                ColumnRef('transfers', 'to_stop_id'),
                ColumnRef('pathways', 'from_stop_id'),
                ColumnRef('pathways', 'to_stop_id')
            ]
        }
    }
