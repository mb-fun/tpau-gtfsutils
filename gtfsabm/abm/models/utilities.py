import logging
import datetime
import pandas as pd
import numpy as np
import os

from activitysim.core import inject
from activitysim.core import pipeline
from activitysim.core import config

logger = logging.getLogger(__name__)

@inject.step()
def filter_trips_by_date(trips, calendar, calendar_dates):
    # removes trips that do not occur on specified date

    # filter calendars on date range
    date = int(config.setting('date'))
    calendar_df = calendar.to_frame().reset_index()
    date_in_range = (calendar_df['start_date'] <= date) & (date <= calendar_df['end_date'])
    calendar_filtered_df = calendar_df[date_in_range]

    # filter calendars on day of week
    dow = dow_from_date(date)
    dow_in_service = calendar_filtered_df[dow] == 1
    calendar_filtered_df = calendar_filtered_df[dow_in_service]

    calendar_filterered_service_ids = calendar_filtered_df['service_id']

    # filter calendar_dates for relevant calendar exceptions
    calendar_dates_df = calendar_dates.to_frame()
    added_on_date = (calendar_dates_df['date'] == date) & (calendar_dates_df['exception_type'] == 1)
    added_service_ids = calendar_dates_df[added_on_date]['service_id']

    removed_on_date = (calendar_dates_df['date'] == date) & (calendar_dates_df['exception_type'] == 2)
    removed_service_ids = calendar_dates_df[removed_on_date]['service_id']

    # union with added service
    calendar_filterered_service_ids = pd.concat([calendar_filterered_service_ids, added_service_ids])

    # setdiff with removed service
    calendar_filterered_service_ids = calendar_filterered_service_ids[~calendar_filterered_service_ids.isin(removed_service_ids)]

    # filter trips and write to table
    trips_df = trips.to_frame()
    trips_filtered_df = trips_df[trips_df['service_id'].isin(calendar_filterered_service_ids)]

    pipeline.replace_table("trips", trips_filtered_df)


def seconds_since_zero(military):
    # assert time_is_valid_format(military), 'Time format invalid for %s, please use HH:MM:SS', military

    t = military.split(':')
    hours = int(t[0])
    minutes = int(t[1])
    seconds = int(t[2])

    return hours * 3600 + minutes * 60 + seconds

def seconds_to_military(seconds_since_zero):
    # returns military time string from "seconds since zero"

    hours, seconds_left = divmod(seconds_since_zero, 3600)
    minutes, seconds = divmod(seconds_left, 60)
    return datetime.time(hours, minutes, seconds).strftime('%H:%M:%S')

def min_miliary_arrival_time(grouped):
    # agg function for min time, accepts grouped Series

    trip_id = grouped.name
    grouped_df = grouped.to_frame()
    grouped_df = grouped_df.assign(seconds_since_zero = lambda df: df[trip_id].transform(lambda t: seconds_since_zero(t)))

    idx_of_min = grouped_df['seconds_since_zero'].idxmin(axis=0)

    return grouped_df.loc[idx_of_min, trip_id]

def max_miliary_arrival_time(grouped):
    # agg function for max time, accepts grouped Series

    trip_id = grouped.name
    grouped_df = grouped.to_frame()
    grouped_df = grouped_df.assign(seconds_since_zero = lambda df: df[trip_id].transform(lambda t: seconds_since_zero(t)))

    idx_of_max = grouped_df['seconds_since_zero'].idxmax(axis=0)

    return grouped_df.loc[idx_of_max, trip_id]


def time_range_in_range(start_time_a, end_time_a, start_time_b, end_time_b):
    return (start_time_b <= start_time_a) & (end_time_a <= end_time_b)

def get_trip_bounds_df(stop_times):
    # returns trip bounds dataframe
    #   index: trip_id
    #   columns: start_time, end_time

    stop_times_df = stop_times.to_frame()
    grouped_arrival_times = stop_times_df.groupby('trip_id')['arrival_time']
    
    min_arrival_times = grouped_arrival_times.agg(min_miliary_arrival_time)
    min_arrival_times = min_arrival_times.rename('start_time')

    max_arrival_times = grouped_arrival_times.agg(max_miliary_arrival_time)
    max_arrival_times = max_arrival_times.rename('end_time')

    return pd.concat([min_arrival_times, max_arrival_times], axis=1)

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


@inject.step()
def prune_unused_trips_everywhere(trips, stop_times, frequencies, attributions):
    # remove trips not in trips table from:
    #   stop_times
    #   frequencies
    #   attributions (optional table)

    trips_df = trips.to_frame().reset_index()
    frequencies_df = frequencies.to_frame()
    frequencies_pruned_df = frequencies_df[frequencies_df['trip_id'].isin(trips_df['trip_id'])]

    stop_times_df = stop_times.to_frame()
    stop_times_pruned_df = stop_times_df[stop_times_df['trip_id'].isin(trips_df['trip_id'])]
    
    pipeline.replace_table("frequencies", frequencies_pruned_df)
    pipeline.replace_table("stop_times", stop_times_pruned_df)

    # prune unused trips from attributions

def get_num_trips_for_frequencies_df(frequencies_df):
    # returns frequencies dataframe with added column:
    #   num_trips: total number of occurring trips 

    return frequencies_df.assign(num_trips= \
        np.floor( \
            (frequencies_df['end_time'].transform(seconds_since_zero) - frequencies_df['start_time'].transform(seconds_since_zero)) \
            / frequencies_df['headway_secs'] \
        )
    )


def get_unwrapped_frequencies_dfs(frequencies, stop_times):
    # returns dataframe with a row for every occurring trip represented by frequencies.txt
    # dataframe returned has frequencies columns with changes/additions:
    #   trip_order: sequence of trip in frequency
    #   start_time -> frequency_start 
    #   end_time -> frequency_end
    #   trip_start, trip_end: individual trip bounds

    frequencies_df = frequencies.to_frame()
    frequencies_trip_count_df = get_num_trips_for_frequencies_df(frequencies_df)

    frequencies_trip_count_df = frequencies_trip_count_df \
        .rename(columns={'start_time': 'frequency_start', 'end_time': 'frequency_end' })

    # expand into row per each occurring trip
    frequencies_trip_count_df['num_trips'] = frequencies_trip_count_df['num_trips'] \
        .transform(lambda x: list(range(int(x))))
        
    frequencies_trip_count_df = frequencies_trip_count_df \
        .rename(columns={'num_trips': 'trip_order'}) \
        .explode('trip_order')

    # calculate start time seconds for each trip
    start_kwargs = { \
        'start_time' : \
            lambda x: \
                x['frequency_start'].transform(seconds_since_zero) + \
                x['trip_order'] * x['headway_secs']
    }
    frequencies_trip_count_df = frequencies_trip_count_df \
        .assign(**start_kwargs)
    frequencies_trip_count_df['start_time'] = frequencies_trip_count_df['start_time'] \
        .transform(int)

    # calculate end time seconds for each trip
    trip_durations_df = get_trip_duration_seconds_series(stop_times)
    frequencies_trip_count_df = frequencies_trip_count_df \
        .merge(trip_durations_df, left_on='trip_id', right_index=True)

    frequencies_trip_count_df = frequencies_trip_count_df.assign( \
            end_time=frequencies_trip_count_df['start_time'] + frequencies_trip_count_df['duration_seconds'] \
        )
    frequencies_trip_count_df['start_time'] = frequencies_trip_count_df['start_time'].transform(seconds_to_military)
    frequencies_trip_count_df['end_time'] = frequencies_trip_count_df['end_time'].transform(seconds_to_military)

    return frequencies_trip_count_df \
        .rename(columns={ 'start_time': 'trip_start', 'end_time': 'trip_end' }) \
        .drop('duration_seconds', axis='columns')


def make_time_ranges_df(time_ranges):
    # Returns dataframe of time_ranges with columns start_time, end_time

    start_times = []
    end_times = []

    for time_range in time_ranges:
        start_times.append(time_range['start'])
        end_times.append(time_range['end'])

    return pd.DataFrame({ 'start_time': start_times, 'end_time': end_times})


def remove_frequencies_with_no_trips_in_range(unwrapped_frequencies_with_range_df, trips, frequencies):
    # Effects:
    #   - Remove entry from frequencies.txt if no trips in range
    #   - Remove trip from trips.txt if trip_id not in any range in frequencies
    # Returns:
    #   - Updated unwrapped_frequencies_with_range_df

    # Remove entry from frequencies.txt if no trips in range
    any_trip_in_frequency_in_range_series = unwrapped_frequencies_with_range_df.groupby(level=['frequency_start', 'trip_id'])['in_range'].any() \
        .rename('any_frequency_trip_in_range')

    # Remove trip from trips.txt if trip_id not in any range in frequencies
    trips_not_in_any_range_series = unwrapped_frequencies_with_range_df.groupby(level=['trip_id'])['in_range'].any()
    trips_not_in_any_range_series = trips_not_in_any_range_series[trips_not_in_any_range_series == False]

    trips_df = trips.to_frame().reset_index()
    trips_filtered_df = trips_df[~trips_df['trip_id'].isin(trips_not_in_any_range_series.index.to_series())]
    
    pipeline.replace_table("trips", trips_filtered_df)

@inject.step()
def filter_repeating_trips_by_time(trips, frequencies, stop_times):
    # edit start_time and end_time of frequencies partially in range (at least one but not all trips occur in range)
    # edit stop_times for trip if start_time has changed

    # do nothing if no repeating trips
    frequencies_df = frequencies.to_frame()
    if (frequencies_df.empty):
        return

    time_ranges = config.setting('time_ranges')
    time_ranges_df = make_time_ranges_df(time_ranges) \
        .rename(columns={ 'start_time': 'range_start', 'end_time': 'range_end' })

    # create long-form dataframe with row for each (trip_id, trip_order, time_range)
    unwrapped_repeating_trips_df = get_unwrapped_frequencies_dfs(frequencies, stop_times)
    range_index_array = time_ranges_df.index.array
    unwrapped_repeating_trips_df['range_index'] = None
    unwrapped_repeating_trips_df['range_index'] = unwrapped_repeating_trips_df['range_index'].apply(lambda x: np.array(range_index_array))

    unwrapped_repeating_trips_df = unwrapped_repeating_trips_df.explode('range_index')

    # HACK I'm not sure why explode is producing duplicates here (1 row explodes to 64 rows instead of 2), but we can remove them here
    unwrapped_repeating_trips_df = unwrapped_repeating_trips_df.drop_duplicates()

    # determine if in ranges
    unwrapped_frequencies_with_ranges_df = unwrapped_repeating_trips_df.merge(time_ranges_df, left_on='range_index', right_index=True)
    kwargs = {'in_range' : lambda x: time_range_in_range( \
        x['trip_start'].transform(seconds_since_zero), \
        x['trip_end'].transform(seconds_since_zero), \
        x['range_start'].transform(seconds_since_zero), \
        x['range_end'].transform(seconds_since_zero)) \
    }
    unwrapped_frequencies_with_ranges_df = unwrapped_frequencies_with_ranges_df.assign(**kwargs)

    unwrapped_frequencies_in_range_df = unwrapped_frequencies_with_ranges_df.groupby(['frequency_start', 'trip_id', 'trip_order'])['in_range'].any()
    unwrapped_frequencies_with_in_range_df = get_unwrapped_frequencies_dfs(frequencies, stop_times).merge(unwrapped_frequencies_in_range_df, on=['frequency_start', 'trip_id', 'trip_order'])

    unwrapped_grouped = unwrapped_frequencies_with_ranges_df.groupby(['frequency_start', 'trip_id'])

    # Remove frequencies with no trips in range
    any_trip_in_frequency_in_range_series = unwrapped_grouped['in_range'].any() \
        .rename('any_frequency_trip_in_range')
    unwrapped_frequencies_with_in_range_df = unwrapped_frequencies_with_in_range_df \
        .merge(any_trip_in_frequency_in_range_series.to_frame(), on=['frequency_start', 'trip_id'])
    unwrapped_frequencies_with_in_range_df = unwrapped_frequencies_with_in_range_df[unwrapped_frequencies_with_in_range_df['any_frequency_trip_in_range'] == True] \
        .drop('any_frequency_trip_in_range', axis='columns')

    # Remove trip from trips.txt if trip_id not in any range in frequencies
    trips_not_in_any_range_series = unwrapped_frequencies_with_in_range_df.groupby(['trip_id'])['in_range'].any()
    trips_not_in_any_range_series = trips_not_in_any_range_series[trips_not_in_any_range_series == False]

    trips_df = trips.to_frame().reset_index()
    trips_filtered_df = trips_df[~trips_df['trip_id'].isin(trips_not_in_any_range_series.index.to_series())]
    
    # Shorten and/or push back frequencies if needed
    unwrapped_grouped = unwrapped_frequencies_with_in_range_df.groupby(['frequency_start', 'trip_id'])
    unwrapped_grouped_last_trip = unwrapped_grouped['trip_order'].max().rename('last_trip_order')

    unwrapped_in_range_only_grouped = unwrapped_frequencies_with_in_range_df[unwrapped_frequencies_with_in_range_df['in_range'] == True].groupby(['frequency_start', 'trip_id'])
    unwrapped_grouped_last_trip_in_range = unwrapped_in_range_only_grouped.apply(lambda g: g[g['trip_order'] == g['trip_order'].max()])
    unwrapped_grouped_last_trip_in_range = unwrapped_grouped_last_trip_in_range[['frequency_start', 'trip_id', 'trip_order', 'trip_end']]
    unwrapped_grouped_last_trip_in_range = unwrapped_grouped_last_trip_in_range \
        .rename(columns={ 'trip_order': 'last_trip_order_in_range', 'trip_end': 'last_trip_end_in_range' }) \
        .reset_index(drop=True)
    
    unwrapped_grouped_first_trip_in_range = unwrapped_in_range_only_grouped.apply(lambda g: g[g['trip_order'] == g['trip_order'].min()])
    unwrapped_grouped_first_trip_in_range = unwrapped_grouped_first_trip_in_range[['frequency_start', 'trip_id', 'trip_order', 'trip_start']]
    unwrapped_grouped_first_trip_in_range = unwrapped_grouped_first_trip_in_range \
        .rename(columns={ 'trip_order': 'first_trip_order_in_range', 'trip_start': 'first_trip_start_in_range' }) \
        .reset_index(drop=True)

    unwrapped_frequencies_with_in_range_df = unwrapped_frequencies_with_in_range_df.merge(unwrapped_grouped_last_trip, left_on=['frequency_start', 'trip_id'], right_on=['frequency_start', 'trip_id'])
    unwrapped_frequencies_with_in_range_df = unwrapped_frequencies_with_in_range_df.merge(unwrapped_grouped_first_trip_in_range, left_on=['frequency_start', 'trip_id'], right_on=['frequency_start', 'trip_id'])
    unwrapped_frequencies_with_in_range_df = unwrapped_frequencies_with_in_range_df.merge(unwrapped_grouped_last_trip_in_range, left_on=['frequency_start', 'trip_id'], right_on=['frequency_start', 'trip_id'])

    unwrapped_frequencies_with_in_range_df.loc[unwrapped_frequencies_with_in_range_df['last_trip_order'] > unwrapped_frequencies_with_in_range_df['last_trip_order_in_range'], \
        'frequency_end'] = unwrapped_frequencies_with_in_range_df['last_trip_end_in_range']
    
    unwrapped_frequencies_with_in_range_df.loc[unwrapped_frequencies_with_in_range_df['first_trip_order_in_range'] > 0, \
        'frequency_start'] = unwrapped_frequencies_with_in_range_df['first_trip_start_in_range']

    unwrapped_frequencies_with_in_range_df = unwrapped_frequencies_with_in_range_df[ \
        (unwrapped_frequencies_with_in_range_df['trip_order'] <= unwrapped_frequencies_with_in_range_df['last_trip_order_in_range']) \
        & (unwrapped_frequencies_with_in_range_df['trip_order'] >= unwrapped_frequencies_with_in_range_df['first_trip_order_in_range']) \
    ]

    # split frequencies on gaps

    # agg individual in_range status
    unwrapped_trips_out_of_range = unwrapped_frequencies_with_in_range_df[unwrapped_frequencies_with_in_range_df['in_range'] == False]

    # copy over index columns we need to iteratively update
    unwrapped_frequencies_with_in_range_df = unwrapped_frequencies_with_in_range_df \
        .reset_index() \
        .set_index(['frequency_start', 'trip_id', 'trip_order'], drop=False)
    unwrapped_frequencies_with_in_range_df = unwrapped_frequencies_with_in_range_df.rename(columns={ \
        'frequency_start': 'new_frequency_start', \
        'trip_order': 'new_trip_order' \
    })

    # Perform update for each out-of-range trip on adjacent in-range trips
    for index, current_row in unwrapped_trips_out_of_range.iterrows():
        cur_frequency_start = current_row['frequency_start']
        cur_trip_id = current_row['trip_id']
        cur_trip_order = current_row['trip_order']

        # if next trip in range
        if (unwrapped_frequencies_with_in_range_df.loc[cur_frequency_start, cur_trip_id, cur_trip_order + 1]['in_range'] == True):
            # update frequency start for all future trips in frequency
            new_trip_start = unwrapped_frequencies_with_in_range_df.loc[ \
                (unwrapped_frequencies_with_in_range_df['new_frequency_start'] == cur_frequency_start) \
                    & (unwrapped_frequencies_with_in_range_df['trip_id'] == cur_trip_id) \
                    & (unwrapped_frequencies_with_in_range_df['new_trip_order'] == cur_trip_order + 1), \
            ]['trip_start'].tolist()[0]
            unwrapped_frequencies_with_in_range_df.loc[ \
                (unwrapped_frequencies_with_in_range_df['new_frequency_start'] == cur_frequency_start) \
                    & (unwrapped_frequencies_with_in_range_df['trip_id'] == cur_trip_id) \
                    & (unwrapped_frequencies_with_in_range_df['new_trip_order'] >= cur_trip_order), \
                'new_frequency_start' \
            ] = new_trip_start

            # update trip order for all future trips in frequency
            unwrapped_frequencies_with_in_range_df['new_trip_order'] = unwrapped_frequencies_with_in_range_df.apply(lambda unwrapped_frequencies_with_in_range_df: \
                unwrapped_frequencies_with_in_range_df['new_trip_order'] - (cur_trip_order + 1) if ( \
                    (unwrapped_frequencies_with_in_range_df['new_frequency_start'] == cur_frequency_start) \
                    & (unwrapped_frequencies_with_in_range_df['trip_id'] == cur_trip_id) \
                    & (unwrapped_frequencies_with_in_range_df['new_trip_order'] >= cur_trip_order) \
                ) else unwrapped_frequencies_with_in_range_df['new_trip_order'], \
            axis='columns')

        # if next trip in range
        if (unwrapped_frequencies_with_in_range_df.loc[cur_frequency_start, cur_trip_id, cur_trip_order - 1]['in_range'] == True):
            # update frequency end for all previous trips in frequency
            new_trip_end = unwrapped_frequencies_with_in_range_df.loc[ \
                (unwrapped_frequencies_with_in_range_df['new_frequency_start'] == cur_frequency_start) \
                    & (unwrapped_frequencies_with_in_range_df['trip_id'] == cur_trip_id) \
                    & (unwrapped_frequencies_with_in_range_df['new_trip_order'] == cur_trip_order - 1), \
            ]['trip_end'].tolist()[0]
            unwrapped_frequencies_with_in_range_df.loc[ \
                (unwrapped_frequencies_with_in_range_df['new_frequency_start'] == cur_frequency_start) \
                    & (unwrapped_frequencies_with_in_range_df['trip_id'] == cur_trip_id) \
                    & (unwrapped_frequencies_with_in_range_df['new_trip_order'] <= cur_trip_order), \
                'frequency_end' \
            ] = new_trip_end


    # Now we can finally remove all out-of-range entries and reshape back into frequencies 
    unwrapped_frequencies_with_in_range_df = unwrapped_frequencies_with_in_range_df[unwrapped_frequencies_with_in_range_df['in_range'] == True]
    unwrapped_frequencies_with_in_range_df = unwrapped_frequencies_with_in_range_df \
        .reset_index(drop=True) \
        .rename(columns={ 'new_frequency_start': 'start_time', 'frequency_end': 'end_time' })
    filtered_frequencies_df = unwrapped_frequencies_with_in_range_df[ \
        [ \
            'trip_id', \
            'start_time', \
            'end_time', \
            'headway_secs', \
            # 'exact_times' \
        ] \
    ]
    filtered_frequencies_df = filtered_frequencies_df.drop_duplicates()


    pipeline.replace_table("trips", trips_filtered_df)
    pipeline.replace_table("frequencies", filtered_frequencies_df)


@inject.step()
def filter_single_trips_by_time(trips, frequencies, stop_times):
    # filters trips by time ranges provided in config
    # trips will only be kept if they start and end within the time range
    # TODO DON'T remove trips in frequencies.txt! stop_times for those trips are irrelevant

    time_ranges = config.setting('time_ranges')
    trip_bounds_df = get_trip_bounds_df(stop_times)

    # is trip in any time range
    for time_range in time_ranges:
        start = time_range['start']
        end = time_range['end']

        time_range_key = 'inrange_' + start + '_' + end

        kwargs = {time_range_key : lambda x: time_range_in_range( \
            x['start_time'].transform(seconds_since_zero), \
            x['end_time'].transform(seconds_since_zero), \
            seconds_since_zero(start), \
            seconds_since_zero(end) \
        )}
        trip_bounds_df = trip_bounds_df.assign(**kwargs)

    # merge results with trips df
    trip_bounds_df = trip_bounds_df \
        .drop('start_time', axis=1) \
        .drop('end_time', axis=1) \
        .any(axis=1) \
        .rename('inrange')
    

    trips_df = trips.to_frame()
    trips_df = trips_df.merge(trip_bounds_df.to_frame(), left_index=True, right_index=True)

    frequencies_df = frequencies.to_frame()
    frequency_trip_ids = frequencies_df['trip_id']

    # filter trips and write to table
    trips_filtered_df = trips_df[(trips_df['inrange'] == True) | trips_df.index.to_series().isin(frequency_trip_ids)]
    trips_filtered_df = trips_filtered_df.drop('inrange', axis=1)


    pipeline.replace_table("trips", trips_filtered_df)

@inject.step()
def calculate_average_headways(trips, stop_times, frequencies, agency, routes, output_dir):
    # For each route, and each specified time period, comma separated values, LF/CR for each new route/time period combo:
    #   Agency ID
    #   Agency Name
    #   Date
    #   Start Time
    #   End Time
    #   Route ID
    #   Route Name
    #   Route Frequency [for specified time period]
    #   Trip Start Time, Trip Start Time, Trip Start Time, … [for each trip that starts during specified time period(s)]

    time_ranges = config.setting('time_ranges')

    frequencies_df = frequencies.to_frame()
    frequency_trip_ids = frequencies_df['trip_id']

    stop_times_df = stop_times.to_frame()
    trip_bounds_df = get_trip_bounds_df(stop_times) \
        .drop('end_time', axis='columns')
    
    # remove stop_times for trips in frequencies as they should be ignored
    trip_bounds_df = trip_bounds_df[~trip_bounds_df.index.to_series().isin(frequency_trip_ids)]

    unwrapped_repeating_trips = get_unwrapped_frequencies_dfs(frequencies, stop_times)
    # unwrapped_repeating_trips['trip_id'] = unwrapped_repeating_trips['trip_id'] + '_' + unwrapped_repeating_trips['trip_order'].astype(str)
    unwrapped_repeating_trips = unwrapped_repeating_trips.drop([ \
        # 'exact_times', \
        'trip_order', 'frequency_start', 'frequency_end', 'headway_secs', 'trip_end'
    ], axis='columns')
    unwrapped_repeating_trips = unwrapped_repeating_trips.rename(columns={ 'trip_start': 'start_time' })
    unwrapped_repeating_trips.set_index('trip_id', inplace=True)

    trips_df = trips.to_frame().reset_index()

    trip_start_times = pd.concat([trip_bounds_df, unwrapped_repeating_trips])
    trip_start_times = trip_start_times.merge(trips_df[['trip_id','route_id', 'direction_id']], how='left', on='trip_id')
    trip_start_times['start_time_seconds'] = trip_start_times['start_time'].transform(seconds_since_zero)

    # calculate deltas
    trip_start_times.sort_values(['route_id', 'direction_id', 'start_time_seconds'], inplace=True)
    trip_start_times['delta_seconds'] = trip_start_times['start_time_seconds'].diff()
    mask = (trip_start_times['route_id'] != trip_start_times['route_id'].shift(1)) \
        | ( \
            ~np.isnan(trip_start_times['direction_id']) \
            & (trip_start_times['direction_id'] != trip_start_times['direction_id'].shift(1)) \
        )

    # TODO: look into the warning that this produces:
    # A value is trying to be set on a copy of a slice from a DataFrame

    # See the caveats in the documentation: http://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
    trip_start_times['delta_seconds'][mask] = np.nan # set first trip delta

    route_avg_headway_secs = trip_start_times \
        .groupby(['route_id', 'direction_id'])['delta_seconds'].mean() \
        .fillna(0) \
        .rename('average_headway_secs')

    route_trip_starts_list = trip_start_times.groupby(['route_id', 'direction_id'])['start_time'].apply(list) \
        .rename('trip_start_times')
    route_avg_headway_data = pd.merge(route_avg_headway_secs, route_trip_starts_list, left_index=True, right_index=True)

    route_avg_headway_data = route_avg_headway_data[['trip_start_times', 'average_headway_secs']]
    route_avg_headway_data = route_avg_headway_data.reset_index()
    route_info = routes.to_frame()[['agency_id', 'route_long_name']]
    agency_info = agency.to_frame()['agency_name']
    route_avg_headway_data = route_avg_headway_data.merge(route_info, on='route_id')
    route_avg_headway_data = route_avg_headway_data.merge(agency_info.to_frame(), on='agency_id')

    route_avg_headway_data['date'] = config.setting('date')
    # TODO: update to support multiple time ranges
    route_avg_headway_data['start_time'] = time_ranges[0]['start']
    route_avg_headway_data['end_time'] = time_ranges[0]['end']

    route_avg_headway_data = route_avg_headway_data.reset_index()

    avg_headways_fname = os.path.join(output_dir, 'average_headways.csv')
    route_avg_headway_data.to_csv(avg_headways_fname, index=False)


def dow_from_date(date):
    # input: Numerical date in YYYYMMDD format
    # returns: day of week as full lowercase string (i.e., "sunday")

    assert date_is_valid_format(date), "Please provide numeric date in YYYYMMDD format"

    datestring = str(date)
    year = datestring[:4]
    month = datestring[4:6]
    day = datestring[-2:]

    weekday = datetime.date(int(year), int(month), int(day)).strftime('%A')
    return weekday.lower()


def date_is_valid_format(date):
    # TODO
    return True

def time_is_valid_format(time):
    # TODO
    return True

def get_set_difference_homogenous_dataframes(source_df, diff_df):
    # returns dataframe of entries in source_df not in diff_df
    return pd.merge(source_df, diff_df, how='outer', indicator=True) \
        .query('_m == "left_only"') \
        .drop(columns=['_m'])