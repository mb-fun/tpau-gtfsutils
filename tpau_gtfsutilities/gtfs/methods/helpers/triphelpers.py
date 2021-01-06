import pandas as pd
import datetime
import numpy as np

from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs as gtfs_singleton

from tpau_gtfsutilities.helpers.datetimehelpers import seconds_since_zero
from tpau_gtfsutilities.helpers.datetimehelpers import seconds_to_military

def get_trip_duration_seconds(gtfs_override=None, trip_bounds=None):
    # returns trip duration series 'duration_seconds'
    gtfs = gtfs_override if gtfs_override else gtfs_singleton

    trip_bounds = trip_bounds if trip_bounds is not None else get_trip_bounds(gtfs_override=gtfs)
    trip_durations_df = trip_bounds.assign( \
        duration_seconds = \
            trip_bounds['end_time'].transform(seconds_since_zero) \
                - trip_bounds['start_time'].transform(seconds_since_zero) \
    )

    return trip_durations_df['duration_seconds']


def get_trip_bounds(gtfs_override=None):
    # returns trip bounds dataframe
    #   index: trip_id
    #   columns: start_time, end_time
    gtfs = gtfs_override if gtfs_override else gtfs_singleton

    stop_times = gtfs.get_table('stop_times')

    def min_miliary_arrival_time(grouped):
        trip_id = grouped.name
        grouped_df = grouped.to_frame()
        
        grouped_df = grouped_df[grouped_df[trip_id] != ''] \
            .assign(seconds_since_zero = lambda df: df[trip_id].transform(lambda t: seconds_since_zero(t)))

        idx_of_min = grouped_df['seconds_since_zero'].idxmin(axis=0)

        return grouped_df.loc[idx_of_min, trip_id]

    def max_miliary_arrival_time(grouped):
        trip_id = grouped.name
        grouped_df = grouped.to_frame()

        grouped_df = grouped_df[grouped_df[trip_id] != ''] \
            .assign(seconds_since_zero = lambda df: df[trip_id].transform(lambda t: seconds_since_zero(t)))

        idx_of_max = grouped_df['seconds_since_zero'].idxmax(axis=0)

        return grouped_df.loc[idx_of_max, trip_id]

    grouped_arrival_times = stop_times[stop_times['arrival_time'].notnull()].groupby('trip_id')['arrival_time']
    
    min_arrival_times = grouped_arrival_times \
        .agg(min_miliary_arrival_time) \
        .rename('start_time')

    max_arrival_times = grouped_arrival_times \
        .agg(max_miliary_arrival_time) \
        .rename('end_time')

    return pd.concat([min_arrival_times, max_arrival_times], axis=1)


def get_trips_extended(gtfs_override=None):
    # returns trips with agency, calendar and time information
    # start_date, end_date
    # daygroups
    # start_time
    # end_time
    # duration
    # is_repeating
    # route_type
    # agency id
    # agency name
    gtfs = gtfs_override if gtfs_override else gtfs_singleton

    trips_extended = gtfs.get_table('trips')

    if gtfs.has_table('calendar'):
        calendar = gtfs.get_table('calendar')
        calendar_info = calendar[ \
            [
                'start_date', 'end_date', \
                'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', \
            ] \
        ]

        trips_extended = trips_extended.reset_index() \
            .merge(calendar_info, how='left', on='service_id').set_index('trip_id')
    
    trip_bounds = get_trip_bounds(gtfs_override=gtfs)
    trips_extended = trips_extended.merge(trip_bounds, left_index=True, right_index=True)
    
    trips_extended = trips_extended.merge(get_trip_duration_seconds(gtfs_override=gtfs, trip_bounds=trip_bounds), left_index=True, right_index=True)

    frequencies = gtfs.get_table('frequencies')
    trips_extended['is_repeating'] = \
        trips_extended.index.to_series().isin(frequencies['trip_id']) \
        if gtfs.has_table('frequencies') else False

    # agency information
    agency = gtfs.get_table('agency')
    if not gtfs.is_multiagency():
        agency_row = agency.iloc[0]
        trips_extended['agency_name'] = agency_row['agency_name']
        if 'agency_id' in agency.columns:
            trips_extended['agency_id'] = agency_row['agency_id']
        else:
            trips_extended['agency_id'] = ''
    else:
        route_agencies = gtfs.get_table('routes')['agency_id']
        trips_extended = trips_extended.reset_index()
        trips_extended = trips_extended.merge( \
            route_agencies,
            how='left',
            left_on='route_id',
            right_index=True
        )
        trips_extended = trips_extended.merge( \
            agency[['agency_id', 'agency_name']],
            how='left',
            left_on='agency_id',
            right_on='agency_id'
        )
        trips_extended = trips_extended.set_index('trip_id')

    route_types = gtfs.get_table('routes')['route_type']
    trips_extended = trips_extended \
        .reset_index() \
        .merge(
            route_types,
            how='left',
            left_on='route_id',
            right_index=True
        ).set_index('trip_id')

    return trips_extended


def get_unwrapped_repeating_trips(gtfs_override=None):
    # returns dataframe with a row for every occurring trip represented by frequencies.txt
    # dataframe returned has frequencies columns with changes/additions:
    #   trip_order: sequence of trip in frequency (starting at 0)
    #   start_time -> frequency_start 
    #   end_time -> frequency_end
    #   trip_start, trip_end: individual trip bounds
    gtfs = gtfs_override if gtfs_override else gtfs_singleton

    if not gtfs.has_table('frequencies'):
        return pd.DataFrame()

    frequencies = gtfs.get_table('frequencies')

    frequencies.rename(columns={'start_time': 'frequency_start', 'end_time': 'frequency_end' }, inplace=True)

    # expand into row per each occurring trip
    frequencies['trip_order'] = np.ceil( \
            (frequencies['frequency_end'].transform(seconds_since_zero) - frequencies['frequency_start'].transform(seconds_since_zero)) \
            / frequencies['headway_secs'].transform(int) \
        ).transform(lambda x: list(range(int(x))))
        
    unwrapped_frequencies = frequencies.explode('trip_order')

    # calculate start time seconds for each trip
    trip_start_kwargs = { \
        'start_time' : \
            lambda x: \
                x['frequency_start'].transform(seconds_since_zero) + \
                x['trip_order'] * x['headway_secs']
    }
    unwrapped_frequencies = unwrapped_frequencies.assign(**trip_start_kwargs)

    # calculate end time seconds for each trip
    unwrapped_frequencies = unwrapped_frequencies.merge(get_trip_duration_seconds(gtfs_override=gtfs), left_on='trip_id', right_index=True)

    unwrapped_frequencies = unwrapped_frequencies.assign( \
            end_time=unwrapped_frequencies['start_time'] + unwrapped_frequencies['duration_seconds'] \
        )
    unwrapped_frequencies['start_time'] = unwrapped_frequencies['start_time'].transform(seconds_to_military)
    unwrapped_frequencies['end_time'] = unwrapped_frequencies['end_time'].transform(seconds_to_military)

    unwrapped_frequencies = unwrapped_frequencies \
        .rename(columns={ 'start_time': 'trip_start', 'end_time': 'trip_end' }) \
        .drop('duration_seconds', axis='columns')

    return unwrapped_frequencies
    
