import pandas as pd
import numpy as np

from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs as gtfs_singleton
from tpau_gtfsutilities.config.utilityoutput import utilityoutput
from tpau_gtfsutilities.gtfs.methods.helpers import triphelpers
from tpau_gtfsutilities.gtfs.methods.helpers.calendarhelpers import GTFSServiceCalendar

def calculate_stop_visits(gtfs_override=None):
    # For each stop, comma separated values, CR/LF for new stop/row
    #     Agency ID
    #     Agency Name
    #     Route ID
    #     Stop ID
    #     Stop Name
    #     Stop Latitude
    #     Stop Longitude
    #     Visit Count [for specified time period]
    #     Board-count [if GTFS-ride input, for specified time period]
    #     Alight-count [if GTFS-ride input, for specified time period, if available]

    gtfs = gtfs_override if gtfs_override else gtfs_singleton

    stops_report = calculate_stop_visits(gtfs_override=gtfs)

    return stops_report

def calculate_stop_visits(gtfs_override=None):
    # use original so report includes stops that have been filtered by polygon or date/time
    
    gtfs = gtfs_override if gtfs_override else gtfs_singleton

    stops = gtfs.get_table('stops', original=True)
    stops = stops[['stop_name', 'stop_lat', 'stop_lon']]

    stop_trip_counts = get_stop_visit_counts(gtfs_override=gtfs)

    # left join visits against all route/stop pairs
    routes = gtfs.get_table('routes', original=True).reset_index()

    # for building cross product
    stops['_key'] = 1
    routes['_key'] = 1

    route_stops = routes.merge( \
        stops.reset_index(),
        on='_key'
    ) \
        .set_index(['route_id', 'stop_id'])
    
    route_stops = route_stops.drop(route_stops.columns, 1) # just keep the index

    stop_trip_counts = route_stops.merge( \
        stop_trip_counts,
        how='left',
        left_index=True,
        right_index=True
    ) \
        .fillna(0) \
        .astype({'visit_counts': 'int32'})

    stops = stops.merge(stop_trip_counts.reset_index(), \
        how='left', \
        left_index=True, \
        right_on='stop_id' \
    ) \
        .fillna(0) \
        .astype({'visit_counts': 'int32'})


    # service date is included here because board/alight information isn't useful if 
    # it is not known to be within specified daterange
    necessary_board_alight_cols = ['service_date', 'boardings', 'alightings']

    if gtfs.has_table('board_alight') and all(col in gtfs.get_columns('board_alight') for col in necessary_board_alight_cols):
        board_alight = gtfs.get_table('board_alight').reset_index()
        trip_routes = gtfs.get_table('trips', column='route_id')
        board_alight = board_alight.merge(trip_routes, how=left, left_on='trip_id', right_index=True)
        stop_boardings = board_alight[['route_id', 'stop_id', 'boardings']].groupby(['route_id', 'stop_id']).sum()
        stop_alightings = board_alight[['route_id', 'stop_id', 'alightings']].groupby(['route_id', 'stop_id']).sum()
        stops = stops.merge(stop_boardings, \
            how='left', \
            right_index=True, \
            left_index=True \
        )
        stops = stops.merge(stop_alightings, \
            how='left', \
            right_index=True, \
            left_index=True \
        )
    else:
        stops['boardings'] = 'N/A'
        stops['alightings'] = 'N/A'

    # add agency info (id already included if multifeed)
    agency = gtfs.get_table('agency')
    agency_row = agency.iloc[0]
    stops['agency_name'] = agency_row['agency_name']
    if 'agency_id' in agency.columns:
        stops['agency_id'] = agency_row['agency_id']
    else:
        stops['agency_id'] = ''

    stops_report = stops.reset_index()[[
        'agency_id',
        'agency_name',
        'route_id',
        'stop_id',
        'stop_name',
        'stop_lat',
        'stop_lon',
        'visit_counts',
        'boardings',
        'alightings'
    ]]

    return stops_report.sort_values(by=['agency_id', 'route_id', 'stop_id'])

def get_stop_visit_counts(gtfs_override=None):
    # get visit counts for route and stop pairs as series

    gtfs = gtfs_override if gtfs_override else gtfs_singleton

    trip_actual_scheduled_stops = gtfs.get_table('stop_times')[['trip_id', 'stop_id']]
    trip_actual_scheduled_stops['visits'] = 1

    trip_scheduled_stops = gtfs.get_table('stop_times', original=True)[['trip_id', 'stop_id']]
    trip_scheduled_stops = trip_scheduled_stops.merge( \
        trip_actual_scheduled_stops,
        how='left',
        on=['trip_id', 'stop_id']
    ).fillna(0)

    has_frequencies = gtfs.has_table('frequencies')
    if has_frequencies:
        unwrapped_repeating_trips = triphelpers.get_unwrapped_repeating_trips(gtfs_override=gtfs)
        repeating_trip_counts = unwrapped_repeating_trips['trip_id'].value_counts().rename('trip_counts')
        trip_scheduled_stops = trip_scheduled_stops.merge( \
            repeating_trip_counts.to_frame(), \
            how='left', \
            left_on='trip_id', \
            right_index=True \
        ) \
        .fillna(1) \
        .astype({'trip_counts': 'int32'})
    else:
        # single trips have a trip_count of 1
        trip_scheduled_stops['trip_counts'] = 1

    trip_service_counts = get_trip_service_counts(gtfs_override=gtfs)

    trip_scheduled_stops_with_service = trip_scheduled_stops.merge( \
        trip_service_counts.rename('active_days').to_frame(), \
        how='left', \
        left_on='trip_id', \
        right_index=True \
    )

    trip_scheduled_stops_with_service['service_trips'] = \
        trip_scheduled_stops_with_service['visits'] * trip_scheduled_stops_with_service['trip_counts'] * trip_scheduled_stops_with_service['active_days']

    # add route_id
    trips = triphelpers.get_trips_extended(gtfs_override=gtfs, original=True)[['agency_id', 'route_id']]

    trip_scheduled_stops_with_service = trip_scheduled_stops_with_service.merge( \
        trips, \
        how='left', \
        left_on='trip_id', \
        right_index=True \
    )

    cols = ['route_id', 'stop_id', 'service_trips', 'agency_id']
    groupby_cols = ['route_id', 'stop_id', 'agency_id']

    stop_service_counts = trip_scheduled_stops_with_service[cols].groupby(groupby_cols).sum()

    return stop_service_counts.rename(columns={'service_trips':'visit_counts'})


def get_trip_service_counts(gtfs_override=None):
    # Returns a series with the number of service days each trip has

    gtfs = gtfs_override if gtfs_override else gtfs_singleton

    trips_extended = triphelpers.get_trips_extended(gtfs_override=gtfs)
    calendar = gtfs.get_table('calendar').reset_index()


    calendar_active_days = calendar['service_id'].to_frame().reset_index()
    
    calendar_active_days['active_days'] = calendar_active_days['service_id'].apply(lambda id: \
        GTFSServiceCalendar(id, gtfs_override=gtfs).num_active_days() \
    )

    # https://stackoverflow.com/questions/11976503/how-to-keep-index-when-using-pandas-merge
    trips_extended = trips_extended.reset_index().merge( \
        calendar_active_days, \
        how='left', \
        on='service_id' \
    ).set_index('trip_id').drop_duplicates()

    return trips_extended['active_days']