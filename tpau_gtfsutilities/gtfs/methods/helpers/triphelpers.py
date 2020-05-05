import pandas as pd
import datetime
import numpy as np

from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs

from tpau_gtfsutilities.helpers.datetimehelpers import seconds_since_zero
from tpau_gtfsutilities.helpers.datetimehelpers import seconds_to_military

def get_trip_duration_seconds():
    # returns trip duration series 'duration_seconds'

    trip_bounds = get_trip_bounds()
    trip_durations_df = trip_bounds.assign( \
        duration_seconds=trip_bounds['end_time'].transform(seconds_since_zero) \
            - trip_bounds['start_time'].transform(seconds_since_zero)
    )

    return trip_durations_df \
        .drop('start_time', axis='columns') \
        .drop('end_time', axis='columns')


def get_trip_bounds():
    # returns trip bounds dataframe
    #   index: trip_id
    #   columns: start_time, end_time

    stop_times = gtfs.get_table('stop_times')

    def min_miliary_arrival_time(grouped):
        trip_id = grouped.name
        grouped_df = grouped.to_frame() \
            .assign(seconds_since_zero = lambda df: df[trip_id].transform(lambda t: seconds_since_zero(t)))

        idx_of_min = grouped_df['seconds_since_zero'].idxmin(axis=0)

        return grouped_df.loc[idx_of_min, trip_id]

    def max_miliary_arrival_time(grouped):
        trip_id = grouped.name
        grouped_df = grouped.to_frame() \
            .assign(seconds_since_zero = lambda df: df[trip_id].transform(lambda t: seconds_since_zero(t)))

        idx_of_max = grouped_df['seconds_since_zero'].idxmax(axis=0)

        return grouped_df.loc[idx_of_max, trip_id]

    grouped_arrival_times = stop_times.groupby('trip_id')['arrival_time']
    
    min_arrival_times = grouped_arrival_times \
        .agg(min_miliary_arrival_time) \
        .rename('start_time')

    max_arrival_times = grouped_arrival_times \
        .agg(max_miliary_arrival_time) \
        .rename('end_time')

    return pd.concat([min_arrival_times, max_arrival_times], axis=1)


def get_trips_extended():
    # returns trips with calendar and time information
    # start_date, end_date
    # daygroups
    # start_time
    # end_time
    # duration
    # is_repeating

    trips = gtfs.get_table('trips')
    calendar = gtfs.get_table('calendar')
    frequencies = gtfs.get_table('frequencies')

    calendar_info = calendar[ \
        [
            'start_date', 'end_date', \
            'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', \
        ] \
    ]

    frequencies_trip_ids = frequencies['trip_id']

    trips_extended = trips.reset_index().merge(calendar_info, how='left', on='service_id') \
        .set_index('trip_id')
    trips_extended = trips_extended.merge(get_trip_bounds(), left_index=True, right_index=True)
    trips_extended = trips_extended.merge(get_trip_duration_seconds(), left_index=True, right_index=True)

    trips_extended['is_repeating'] = trips_extended.index.to_series().isin(frequencies_trip_ids)

    return trips_extended


def get_unwrapped_repeating_trips():
    # returns dataframe with a row for every occurring trip represented by frequencies.txt
    # dataframe returned has frequencies columns with changes/additions:
    #   trip_order: sequence of trip in frequency
    #   start_time -> frequency_start 
    #   end_time -> frequency_end
    #   trip_start, trip_end: individual trip bounds

    frequencies = gtfs.get_table('frequencies')
    frequencies['num_trips'] = np.floor( \
        (frequencies['end_time'].transform(seconds_since_zero) - frequencies['start_time'].transform(seconds_since_zero)) \
        / frequencies['headway_secs'].transform(int) \
    )

    frequencies = frequencies \
        .rename(columns={'start_time': 'frequency_start', 'end_time': 'frequency_end' })

    # expand into row per each occurring trip
    frequencies['num_trips'] = frequencies['num_trips'] \
        .transform(lambda x: list(range(int(x))))
        
    unwrapped_frequencies = frequencies \
        .rename(columns={'num_trips': 'trip_order'}) \
        .explode('trip_order')

    # calculate start time seconds for each trip
    start_kwargs = { \
        'start_time' : \
            lambda x: \
                x['frequency_start'].transform(seconds_since_zero) + \
                x['trip_order'] * x['headway_secs']
    }
    unwrapped_frequencies = unwrapped_frequencies \
        .assign(**start_kwargs)
    unwrapped_frequencies['start_time'] = unwrapped_frequencies['start_time'] \
        .transform(int)

    # calculate end time seconds for each trip
    unwrapped_frequencies = unwrapped_frequencies \
        .merge(get_trip_duration_seconds(), left_on='trip_id', right_index=True)

    unwrapped_frequencies = unwrapped_frequencies.assign( \
            end_time=unwrapped_frequencies['start_time'] + unwrapped_frequencies['duration_seconds'] \
        )
    unwrapped_frequencies['start_time'] = unwrapped_frequencies['start_time'].transform(seconds_to_military)
    unwrapped_frequencies['end_time'] = unwrapped_frequencies['end_time'].transform(seconds_to_military)

    unwrapped_frequencies = unwrapped_frequencies \
        .rename(columns={ 'start_time': 'trip_start', 'end_time': 'trip_end' }) \
        .drop('duration_seconds', axis='columns')

    return unwrapped_frequencies
    
