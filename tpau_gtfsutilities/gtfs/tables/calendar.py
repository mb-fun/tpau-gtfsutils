from .gtfstable import GTFSTable
from .helpers import ColumnRef

class Calendar(GTFSTable):
    index=['service_id']
    downstream_columns = {
        'service_id': {
            'references': [ColumnRef('trips', 'service_id')],
            'with_table_col': ('calendar_dates', 'service_id')
        }
    }
