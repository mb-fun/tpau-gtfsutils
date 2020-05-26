from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs

def prune_unused_trips():
    # remove trips not in trips table from:
    #   stop_times
    #   frequencies
    #   attributions (optional table)

    # prune_trips_from_frequencies()
    # prune_trips_from_stop_times()
    prune_table_from_table_column('frequencies', 'trips', 'trip_id')
    prune_table_from_table_column('stop_times', 'trips', 'trip_id')

    # TODO prune unused trips from attributions

def prune_table_from_table_column(target, source, column, columns={}):
    # remove rows from target if column value not present in same column of source,  e.g.:
    #   prune_table_from_table_column('frequencies', 'trips', 'trip_id')
    # removes rows from frequencies if trip_id is not present in trips. 
    # column arg is omitted if columns dict is used to use columns with different names, e.g:
    #   prune_table_from_table_column('transfers', 'stops', columns={'transfers': 'from_stop_id', 'stops': 'stop_id})

    target_df = gtfs.get_table(target, index=False)
    source_df = gtfs.get_table(source, index=False)

    target_col = column if not columns else columns[target]
    source_col = column if not columns else columns[source]

    if not (target_df.empty):
        target_pruned = target_df[target_df[target_col].isin(source_df[source_col])]
        gtfs.update_table(target, target_pruned)


def prune_unused_calendars():
    prune_table_from_table_column('calendar', 'trips', 'service_id') 
    prune_table_from_table_column('calendar_dates', 'calendar', 'service_id') 
