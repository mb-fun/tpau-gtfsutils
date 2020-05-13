from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs

def prune_unused_trips_everywhere():
    # remove trips not in trips table from:
    #   stop_times
    #   frequencies
    #   attributions (optional table)

    prune_trips_from_frequencies()
    prune_trips_from_stop_times()

    # TODO prune unused trips from attributions

def prune_trips_from_frequencies():
    trips = gtfs.get_table('trips', index=False)

    frequencies = gtfs.get_table('frequencies')

    if not frequencies.empty:
        frequencies_pruned = frequencies[frequencies['trip_id'].isin(trips['trip_id'])]
        gtfs.update_table('frequencies', frequencies_pruned)

def prune_trips_from_stop_times():
    trips = gtfs.get_table('trips', index=False)

    stop_times = gtfs.get_table('stop_times')
    stop_times_pruned = stop_times[stop_times['trip_id'].isin(trips['trip_id'])]

    gtfs.update_table('stop_times', stop_times_pruned)
