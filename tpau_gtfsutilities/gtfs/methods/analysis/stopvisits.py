import pandas as pd
import numpy as np

from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs as gtfs_singleton
from tpau_gtfsutilities.config.utilityoutput import utilityoutput
from tpau_gtfsutilities.gtfs.methods.helpers import triphelpers
from tpau_gtfsutilities.gtfs.methods.helpers.calendarhelpers import GTFSServiceCalendar

def calculate_stop_visits(gtfs_override=None, include_removed_stops=True):
    # For each stop, comma separated values, CR/LF for new stop/row
    #     Agency ID (if aggregate=False)
    #     Agency Name (if aggregate=False)
    #     Stop ID
    #     Stop Name
    #     Stop Latitude
    #     Stop Longitude
    #     Visit Count [for specified time period]
    #     Board-count [if GTFS-ride input, for specified time period]
    #     Alight-count [if GTFS-ride input, for specified time period, if available]

    gtfs = gtfs_override if gtfs_override else gtfs_singleton

    if not gtfs.is_multiagency():
        stops_report = calculate_stop_visits_single_agency(gtfs_override=gtfs, include_removed_stops=include_removed_stops)
    else:
        stops_report = calculate_stop_visits_multi_agency(gtfs_override=gtfs, include_removed_stops=include_removed_stops)

    return stops_report

def calculate_stop_visits_single_agency(gtfs_override=None, include_removed_stops=True):
    # use original so report includes stops that have been filtered by polygon or date/time
    
    gtfs = gtfs_override if gtfs_override else gtfs_singleton

    if include_removed_stops:
        stops = gtfs.get_table('stops', original=True)
    else:
        stops = gtfs.get_table('stops')
    stops = stops[['stop_name', 'stop_lat', 'stop_lon']]

    stop_trip_counts = get_stop_visit_counts(gtfs_override=gtfs)

    stops = stops.merge(stop_trip_counts, \
        how='left', \
        right_index=True, \
        left_index=True \
    ) \
        .fillna(0) \
        .astype({'visit_counts': 'int32'})


    # service date is included here because board/alight information isn't useful if 
    # it is not known to be within specified daterange
    necessary_board_alight_cols = ['service_date', 'boardings', 'alightings']

    if gtfs.has_table('board_alight') and all(col in gtfs.get_columns('board_alight') for col in necessary_board_alight_cols):
        board_alight = gtfs.get_table('board_alight')
        stop_boardings = board_alight[['stop_id', 'boardings']].groupby(['stop_id']).sum()
        stop_alightings = board_alight[['stop_id', 'alightings']].groupby(['stop_id']).sum()
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
        'stop_id',
        'stop_name',
        'stop_lat',
        'stop_lon',
        'visit_counts',
        'boardings',
        'alightings'
    ]]

    return stops_report


def calculate_stop_visits_multi_agency(gtfs_override=None, include_removed_stops=True):
    # use original so report includes stops that have been filtered by polygon or date/time
    gtfs = gtfs_override if gtfs_override else gtfs_singleton

    if include_removed_stops:
        stops = gtfs.get_table('stops', original=True)
    else:
        stops = gtfs.get_table('stops')
    stops = stops[['stop_name', 'stop_lat', 'stop_lon']]
    agency = gtfs.get_table('agency')[['agency_id', 'agency_name']]

    # for building cross product
    stops['key'] = 1
    agency['key'] = 1

    agency_stops = agency.merge( \
        stops.reset_index(),
        on='key'
    ).drop(columns=['key']).set_index(['agency_id', 'stop_id'])

    stop_visits = get_multi_agency_stop_visit_counts(gtfs_override=gtfs)

    agency_stops = agency_stops.merge(stop_visits, how='left', left_index=True, right_index=True).fillna(0)

    # service date is included here because board/alight information isn't useful if 
    # it is not known to be within specified daterange
    necessary_board_alight_cols = ['service_date', 'boardings', 'alightings']

    if gtfs.has_table('board_alight') and all(col in gtfs.get_columns('board_alight') for col in necessary_board_alight_cols):
        trips_extended = triphelpers.get_trips_extended(gtfs_override=gtfs)
        board_alight = gtfs.get_table('board_alight')
        board_alight = board_alight.merge( \
            trips_extended['agency_id'],
            how='left',
            left_on='trip_id',
            right_index=True
        )
        stop_boardings = board_alight[['stop_id', 'boardings']].groupby(['agency_id', 'stop_id']).sum()
        stop_alightings = board_alight[['stop_id', 'alightings']].groupby(['agency_id', 'stop_id']).sum()
        agency_stops = agency_stops.merge(stop_boardings, \
            how='left', \
            right_index=True, \
            left_index=True \
        )
        agency_stops = agency_stops.merge(stop_alightings, \
            how='left', \
            right_index=True, \
            left_index=True \
        )
    else:
        agency_stops['boardings'] = 'N/A'
        agency_stops['alightings'] = 'N/A'

    agency_stops_report = agency_stops.reset_index()[[
        'agency_id',
        'agency_name',
        'stop_id',
        'stop_name',
        'stop_lat',
        'stop_lon',
        'visit_counts',
        'boardings',
        'alightings'
    ]]

    return agency_stops_report


def get_multi_agency_stop_visit_counts(gtfs_override=None):
    # get visit counts for unique agency/stop combinations as series

    gtfs = gtfs_override if gtfs_override else gtfs_singleton

    trip_scheduled_stops = gtfs.get_table('stop_times')[['trip_id', 'stop_id', 'arrival_time', 'departure_time']]
    trip_stop_pairs = trip_scheduled_stops[['trip_id', 'stop_id']]

    trips_extended = triphelpers.get_trips_extended(gtfs_override=gtfs)
    agency_trip_stops = trip_stop_pairs.merge( \
        trips_extended['agency_id'],
        how='left',
        left_on='trip_id',
        right_index=True,
    )

    has_frequencies = gtfs.has_table('frequencies')
    if has_frequencies:
        unwrapped_repeating_trips = triphelpers.get_unwrapped_repeating_trips(gtfs_override=gtfs)
        repeating_trip_counts = unwrapped_repeating_trips['trip_id'].value_counts().rename('trip_counts')
        agency_trip_stops = agency_trip_stops.merge( \
            repeating_trip_counts.to_frame(), \
            how='left', \
            left_on='trip_id', \
            right_index=True \
        ).fillna(1)
    else:
        # single trips have a trip_count of 1
        agency_trip_stops['trip_counts'] = 1

    trip_service_counts = get_trip_service_counts(gtfs_override=gtfs)

    agency_trip_stops_with_service = agency_trip_stops.merge( \
        trip_service_counts.rename('active_days').to_frame(), \
        how='left', \
        left_on='trip_id', \
        right_index=True \
    )

    agency_trip_stops_with_service['service_trips'] = \
        agency_trip_stops_with_service['trip_counts'] * agency_trip_stops_with_service['active_days']
    
    stop_service_counts = agency_trip_stops_with_service[['agency_id', 'stop_id', 'service_trips']].groupby(['agency_id','stop_id']).sum()


    return stop_service_counts.rename(columns={'service_trips':'visit_counts'})

def get_stop_visit_counts(gtfs_override=None):
    # get visit counts for stops as series

    gtfs = gtfs_override if gtfs_override else gtfs_singleton

    trip_scheduled_stops = gtfs.get_table('stop_times')[['trip_id', 'stop_id', 'arrival_time', 'departure_time']]
    trip_scheduled_stops = trip_scheduled_stops[['trip_id', 'stop_id']]

    has_frequencies = gtfs.has_table('frequencies')
    if has_frequencies:
        unwrapped_repeating_trips = triphelpers.get_unwrapped_repeating_trips(gtfs_override=gtfs)
        repeating_trip_counts = unwrapped_repeating_trips['trip_id'].value_counts().rename('trip_counts')
        trip_scheduled_stops = trip_scheduled_stops.merge( \
            repeating_trip_counts.to_frame(), \
            how='left', \
            left_on='trip_id', \
            right_index=True \
        ).fillna(1)
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
        trip_scheduled_stops_with_service['trip_counts'] * trip_scheduled_stops_with_service['active_days']
        
    stop_service_counts = trip_scheduled_stops_with_service[['stop_id', 'service_trips']].groupby(['stop_id']).sum()

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