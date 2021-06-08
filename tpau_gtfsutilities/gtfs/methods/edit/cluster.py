import numpy as np
import pandas as pd
import geopandas as gpd

from tpau_gtfsutilities.gtfs.gtfscollectionsingleton import gtfs_collection
from tpau_gtfsutilities.gtfs.methods.helpers.triphelpers import get_trips_extended
from tpau_gtfsutilities.gtfs.methods.analysis.stopvisits import calculate_stop_visits

def cluster_stops(radius):
    clusters = get_clusters(radius)

    taps = get_new_taps(clusters)

    cluster_taps = clusters.reset_index().merge( \
        taps.reset_index().rename(columns={'stop_id':'tap_id'})[['old_feed', 'old_stop_id', 'tap_id']],
        how='left',
        left_on=['cluster_feed', 'cluster_stop_id'],
        right_on=['old_feed', 'old_stop_id'],
    ).set_index(['feed', 'stop_id'])['tap_id']

    for feed in gtfs_collection.feeds.keys():
        gtfs = gtfs_collection.feeds[feed]
        
        add_taps_to_stops(gtfs, feed, taps, cluster_taps)
        replace_clustered_stops_everywhere(gtfs, feed, cluster_taps)
        remove_clustered_stops_from_stops(gtfs, feed, cluster_taps)


def get_new_taps(clusters):
    # clusters is a df with columns (cluster_feed, cluster_stop_id)
    # returns df of new tap stops that use new ids and reuses information from cluster stop:
    #   index: old_feed, old_stop_id
    #   columns: same as gtfs_collection.get_combined_gtfs_table('stops')

    cluster_tap_stop_ids = clusters.drop_duplicates().dropna()

    stops = gtfs_collection.get_combined_gtfs_table('stops')
    cluster_tap_stops = stops.merge( \
        cluster_tap_stop_ids.reset_index()[['cluster_feed', 'cluster_stop_id']],
        how='inner',
        left_on=['feed', 'stop_id'],
        right_on=['cluster_feed', 'cluster_stop_id']
    )

    cluster_tap_stops['tap_id'] = cluster_tap_stops.reset_index().apply(lambda row: str(row['index']) + '_TAP_' + str(row['stop_id']), axis=1)
    cluster_tap_stops = cluster_tap_stops.reset_index().rename(columns={
        'feed': 'old_feed',
        'stop_id': 'old_stop_id',
        'tap_id': 'stop_id'
    }).set_index(['old_feed', 'old_stop_id'])

    return cluster_tap_stops

def get_clusters(radius):
    # radius in miles
    # returns df with (feed, stop_id) index of cluster_feed, cluster_stop_id to cluster to, in any
    # if the cluster stop_id is the same as the stop_id, then that stop is a cluster stop

    stops = gtfs_collection.get_combined_gtfs_table('stops')

    stops_gdf = gpd.GeoDataFrame(stops, geometry=gpd.points_from_xy(stops['stop_lon'], stops['stop_lat']), crs='epsg:4326')
    stops_gdf = stops_gdf.to_crs(epsg=2992)
     
    radius_in_feet = float(radius) * 5280
    stops_gdf['buffer'] = stops_gdf['geometry'].buffer(radius_in_feet)

    # will populate with stop to cluster into, if any
    stops_gdf['cluster_stop_id'] = np.nan
    stops_gdf['dist'] = np.nan

    # cluster to premium mode stops first with priority to: 1. lift, 2. rail and 3. tram
    stop_modes = gtfs_collection.get_combined_computed_table(get_stop_modes)
    stops_gdf = stops_gdf.merge( \
        stop_modes,
        how='left',
        left_on=['stop_id', 'feed'],
        right_on=['stop_id', 'feed']
    )

    stops_gdf = sort_stops_by_visits(stops_gdf)

    lift_stops = stops_gdf[stops_gdf['lift']]
    rail_stops = stops_gdf[stops_gdf['rail']]
    tram_stops = stops_gdf[stops_gdf['tram']]
    stops_gdf['not_premium'] = stops_gdf.apply(lambda row: (not row['tram'] and not row['lift'] and not row['rail']), axis=1)
    other_stops = stops_gdf[stops_gdf['not_premium']]

    def cluster_nearby_stops(stops_slice):
        # could probably be faster to compute in df rather than iterating 

        for cand_idx, cand in stops_slice.iterrows():
            for stop_idx, stop in stops_gdf.iterrows():
                not_clustered = type(cand['cluster_stop_id']) == float and np.isnan(cand['cluster_stop_id'])
                cand_available = cand['stop_id'] != stop['stop_id'] and not_clustered
                if cand['stop_id'] != stop['stop_id'] and cand_available and not_clustered and cand['buffer'].contains(stop['geometry']):
                    stops_gdf.loc[cand_idx, 'cluster_stop_id'] = cand['stop_id']
                    stops_gdf.loc[stop_idx, 'cluster_stop_id'] = cand['stop_id']
                    stops_gdf.loc[cand_idx, 'cluster_feed'] = cand['feed']
                    stops_gdf.loc[stop_idx, 'cluster_feed'] = cand['feed']
    
    cluster_nearby_stops(lift_stops)
    cluster_nearby_stops(rail_stops)
    cluster_nearby_stops(tram_stops)
    cluster_nearby_stops(other_stops)

    return stops_gdf[['feed', 'stop_id', 'cluster_stop_id', 'cluster_feed']].set_index(['feed', 'stop_id'])

def sort_stops_by_visits(stops_df):

    stop_visits = gtfs_collection.get_combined_computed_table(lambda gtfs: calculate_stop_visits(gtfs_override=gtfs))
    stop_visits = stop_visits[['feed', 'stop_id', 'visit_counts']]

    stop_visits = stop_visits.groupby(['feed', 'stop_id']).sum('visit_counts')

    stops_df = stops_df.merge( \
        stop_visits,
        how='left',
        left_on=['feed','stop_id'],
        right_index=True
    )

    stops_df = stops_df.sort_values(by=['visit_counts'])

    return stops_df.drop(columns=['visit_counts'])


def get_stop_modes(gtfs):
    # returns df of stop mode usage for tram, rail and lift

    trip_scheduled_stops = gtfs.get_table('stop_times')[['trip_id', 'stop_id', 'arrival_time', 'departure_time']]
    trip_stop_pairs = trip_scheduled_stops[['trip_id', 'stop_id']]

    trips_extended = get_trips_extended(gtfs_override=gtfs)

    trips_extended = trips_extended.reset_index()

    trip_stop_pairs = trip_stop_pairs.merge(
        trips_extended[['trip_id', 'route_type']],
        how='left',
        left_on='trip_id',
        right_on='trip_id',
    )

    stop_mode_list = trip_stop_pairs[['stop_id', 'route_type']].drop_duplicates() \
        .groupby('stop_id')['route_type'] \
        .agg(list) \
        .rename('modes')

    stops = gtfs.get_table('stops')
    stops = stops.merge( \
        stop_mode_list,
        how='left',
        left_index=True,
        right_index=True,
    )

    # route_types for premium modes
    tram = 0
    rail = 2
    lift = 6

    stops['tram'] = stops.apply(lambda row: tram in row['modes'], axis=1)
    stops['rail'] = stops.apply(lambda row: rail in row['modes'], axis=1)
    stops['lift'] = stops.apply(lambda row: lift in row['modes'], axis=1)

    return stops[['tram', 'rail', 'lift']]


def add_taps_to_stops(gtfs, feed, taps, cluster_taps):
    # taps is a dataframe of stops data with union-ed columns across all feeds in collection (with the addition of feed)
    # cluster_taps is a series of tap stop ids (tap_id) for clustered (feed, stop_id) entries

    stops = gtfs.get_table('stops')

    feed_clusters = cluster_taps.loc[feed]
    feed_taps = taps[taps['stop_id'].isin(feed_clusters)] \
        .set_index('stop_id')

    stops_with_taps = pd.concat( \
        [stops, feed_taps],
        axis=0,
        ignore_index=False
    )

    gtfs.update_table('stops', stops_with_taps)


def remove_clustered_stops_from_stops(gtfs, feed, cluster_taps):
    # feed is the name of the feed
    # cluster_taps is a series of tap stop ids (tap_id) for clustered (feed, stop_id) entries
    # if a stop is not clustered, the tap_id will be nan

    clustered_stops = cluster_taps.dropna().reset_index()
    clustered_stops = clustered_stops[clustered_stops['feed'] == feed]

    stops = gtfs.get_table('stops', index=False)
    stops_clustered_removed = stops[~stops['stop_id'].isin(clustered_stops['stop_id'])]

    gtfs.update_table('stops', stops_clustered_removed)


def replace_clustered_stops_everywhere(gtfs, feed, cluster_taps):
    # Replace values in
    # - stops.parent_station
    # - stop_times
    # - transfers
    # - board_alight (ride)
    # - ridership (ride)
    # Tables that may not be included:
    # - pathways
    # - translations

    feed_clusters = cluster_taps.loc[feed]

    # assign missing taps to old ids
    feed_clusters = feed_clusters.fillna(feed_clusters.index.to_series())

    replace_old_with_new_in_table_column(gtfs, 'stops', 'parent_station', feed_clusters)
    replace_old_with_new_in_table_column(gtfs, 'stop_times', 'stop_id', feed_clusters)
    replace_old_with_new_in_table_column(gtfs, 'transfers', 'stop_id', feed_clusters)
    replace_old_with_new_in_table_column(gtfs, 'board_alight', 'stop_id', feed_clusters)
    replace_old_with_new_in_table_column(gtfs, 'ridership', 'stop_id', feed_clusters)


def replace_old_with_new_in_table_column(gtfs, table, column, replacements):
    # replacements is a series of new values (indexed by old value)

    if (not gtfs.has_table(table) or not gtfs.table_has_column(table, column)):
        return

    df = gtfs.get_table(table, index=False)

    old_column = column + '_old'
    column_rename = {}
    column_rename[column] = column + '_old'

    df = df.rename(columns=column_rename)
    replacements = replacements.rename(column)

    df = df.merge( \
        replacements,
        how='left',
        left_on=old_column,
        right_index=True
    )

    gtfs.update_table(table, df)