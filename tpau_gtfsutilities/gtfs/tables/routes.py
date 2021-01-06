from .gtfstable import GTFSTable
from .helpers import ColumnRef

class Routes(GTFSTable):
    index=['route_id']
    downstream_columns = {
        'route_id': {
            'references': [
                ColumnRef('trips', 'route_id'), 
                ColumnRef('fare_rules', 'route_id'), 
                ColumnRef('attributions', 'route_id')
            ]
        }
    }
