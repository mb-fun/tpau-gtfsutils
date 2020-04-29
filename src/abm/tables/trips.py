import logging
import pandas as pd
import datetime
import numpy as np
from activitysim.core import inject
from activitysim.core import config
from activitysim.core.input import read_input_table

logger = logging.getLogger(__name__)


def get_trip_bounds_df(stop_times):
    # returns trip bounds dataframe
    #   index: trip_id
    #   columns: start_time, end_time

    stop_times_df = stop_times.to_frame()
    grouped_arrival_times = stop_times_df.groupby('trip_id')['arrival_time']
    
    min_arrival_times = grouped_arrival_times \
        .agg(min_miliary_arrival_time) \
        .rename('start_time')

    max_arrival_times = grouped_arrival_times \
        .agg(max_miliary_arrival_time) \
        .rename('end_time')

    return pd.concat([min_arrival_times, max_arrival_times], axis=1)


def seconds_to_military(seconds_since_zero):
    # returns military time string from "seconds since zero"

    hours, seconds_left = divmod(seconds_since_zero, 3600)
    minutes, seconds = divmod(seconds_left, 60)
    return datetime.time(hours, minutes, seconds).strftime('%H:%M:%S')


def seconds_since_zero(military):
    # assert time_is_valid_format(military), 'Time format invalid for %s, please use HH:MM:SS', military

    t = military.split(':')
    hours = int(t[0])
    minutes = int(t[1])
    seconds = int(t[2])

    return hours * 3600 + minutes * 60 + seconds


def get_trip_duration_seconds_series(stop_times):
    # returns trip duration series 'duration_seconds'

    trip_bounds_df = get_trip_bounds_df(stop_times)
    trip_durations_df = trip_bounds_df.assign( \
        duration_seconds=trip_bounds_df['end_time'].transform(seconds_since_zero) \
            - trip_bounds_df['start_time'].transform(seconds_since_zero)
    )

    return trip_durations_df \
        .drop('start_time', axis='columns') \
        .drop('end_time', axis='columns')


def min_miliary_arrival_time(grouped):
    # agg function for min time, accepts grouped Series

    trip_id = grouped.name
    grouped_df = grouped.to_frame() \
        .assign(seconds_since_zero = lambda df: df[trip_id].transform(lambda t: seconds_since_zero(t)))

    idx_of_min = grouped_df['seconds_since_zero'].idxmin(axis=0)

    return grouped_df.loc[idx_of_min, trip_id]

def max_miliary_arrival_time(grouped):
    # agg function for max time, accepts grouped Series

    trip_id = grouped.name
    grouped_df = grouped.to_frame() \
        .assign(seconds_since_zero = lambda df: df[trip_id].transform(lambda t: seconds_since_zero(t)))

    idx_of_max = grouped_df['seconds_since_zero'].idxmax(axis=0)

    return grouped_df.loc[idx_of_max, trip_id]


@inject.injectable()
def trips_extended(trips, calendar, frequencies, stop_times):
    # returns trips with calendar and time information
    # start_date, end_date
    # daygroups
    # start_time
    # end_time
    # duration
    # is_repeating

    trips_df = trips.to_frame()
    calendar_df = calendar.to_frame()
    frequencies_df = frequencies.to_frame()

    calendar_info = calendar_df[ \
        [
            'start_date', 'end_date', \
            'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', \
        ] \
    ]
    frequencies_trip_ids = frequencies_df['trip_id']
    trip_bounds_df = get_trip_bounds_df(stop_times)
    trip_duration_seconds = get_trip_duration_seconds_series(stop_times)

    trips_extended_df = trips_df.reset_index().merge(calendar_info, how='left', on='service_id') \
        .set_index('trip_id')
    trips_extended_df = trips_extended_df.merge(trip_bounds_df, left_index=True, right_index=True)
    trips_extended_df = trips_extended_df.merge(trip_duration_seconds, left_index=True, right_index=True)

    trips_extended_df['is_repeating'] = trips_extended_df.index.to_series().isin(frequencies_trip_ids)

    # inject.add_table('trips_extended', trips_extended_df, replace=True)

    return trips_extended_df


def get_num_trips_for_frequencies(frequencies):
    # returns frequencies dataframe with added column:
    #   num_trips: total number of occurring trips 

    frequencies_df = frequencies.to_frame()

    return np.floor( \
        (frequencies_df['end_time'].transform(seconds_since_zero) - frequencies_df['start_time'].transform(seconds_since_zero)) \
        / frequencies_df['headway_secs'].transform(int) \
    )

@inject.injectable()
def unwrapped_repeating_trips(frequencies, stop_times):
    # returns dataframe with a row for every occurring trip represented by frequencies.txt
    # dataframe returned has frequencies columns with changes/additions:
    #   trip_order: sequence of trip in frequency
    #   start_time -> frequency_start 
    #   end_time -> frequency_end
    #   trip_start, trip_end: individual trip bounds

    frequencies_df = frequencies.to_frame()
    frequencies_df['num_trips'] = get_num_trips_for_frequencies(frequencies)

    frequencies_df = frequencies_df \
        .rename(columns={'start_time': 'frequency_start', 'end_time': 'frequency_end' })

    # expand into row per each occurring trip
    frequencies_df['num_trips'] = frequencies_df['num_trips'] \
        .transform(lambda x: list(range(int(x))))
        
    unwrapped_frequencies_df = frequencies_df \
        .rename(columns={'num_trips': 'trip_order'}) \
        .explode('trip_order')

    # calculate start time seconds for each trip
    start_kwargs = { \
        'start_time' : \
            lambda x: \
                x['frequency_start'].transform(seconds_since_zero) + \
                x['trip_order'] * x['headway_secs']
    }
    unwrapped_frequencies_df = unwrapped_frequencies_df \
        .assign(**start_kwargs)
    unwrapped_frequencies_df['start_time'] = unwrapped_frequencies_df['start_time'] \
        .transform(int)

    # calculate end time seconds for each trip
    trip_durations_df = get_trip_duration_seconds_series(stop_times)
    unwrapped_frequencies_df = unwrapped_frequencies_df \
        .merge(trip_durations_df, left_on='trip_id', right_index=True)

    unwrapped_frequencies_df = unwrapped_frequencies_df.assign( \
            end_time=unwrapped_frequencies_df['start_time'] + unwrapped_frequencies_df['duration_seconds'] \
        )
    unwrapped_frequencies_df['start_time'] = unwrapped_frequencies_df['start_time'].transform(seconds_to_military)
    unwrapped_frequencies_df['end_time'] = unwrapped_frequencies_df['end_time'].transform(seconds_to_military)

    unwrapped_frequencies_df = unwrapped_frequencies_df \
        .rename(columns={ 'start_time': 'trip_start', 'end_time': 'trip_end' }) \
        .drop('duration_seconds', axis='columns')

    # inject.add_table('unwrapped_repeating_trips', unwrapped_frequencies_df, replace=True)

    return unwrapped_frequencies_df
    
