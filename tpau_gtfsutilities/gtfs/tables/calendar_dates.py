from .gtfstable import GTFSTable
from .helpers import ColumnRef

class CalendarDates(GTFSTable):
    downstream_columns = {
        'service_id': {
            'references': [ColumnRef('trips', 'service_id')],
            'with_table_col': ('calendar', 'service_id')
        }
    }
