class ColumnRef:
    table = ''
    column = ''
    cascade_row = None

    def __init__(self, table, column, cascade_row=True):
        # cascade_row=True means that row in table should be removed
        # if value in column that owns reference is not found. 
        # i.e, reference from stop_times.stop_id to stops.stop_id has cascade_row=True,
        # since stops should be pruned if not used in trips, and same for the reverse. But a 
        # reference from stops.stop_id to stops.parent_station has cascade_row=False
        self.table = table
        self.column = column
        self.cascade_row = cascade_row
