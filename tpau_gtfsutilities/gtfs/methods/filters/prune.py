from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs

def prune_unused_trips_everywhere():
    # remove trips not in trips table from:
    #   stop_times
    #   frequencies
    #   attributions (optional table)

    trips = gtfs.get_table('trips', index=False)
    frequencies = gtfs.get_table('frequencies')
    frequencies_pruned = frequencies[frequencies['trip_id'].isin(trips['trip_id'])]

    stop_times = gtfs.get_table('stop_times')
    stop_times_pruned = stop_times[stop_times['trip_id'].isin(trips['trip_id'])]
    
    gtfs.update_table('frequencies', frequencies_pruned)
    gtfs.update_table('stop_times', stop_times_pruned)

    # TODO prune unused trips from attributions