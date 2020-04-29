import logging
import datetime
import pandas as pd
import numpy as np
import os

from activitysim.core import inject
from activitysim.core import pipeline
from activitysim.core import config
from src.core.utilityconfig import utilityconfig

logger = logging.getLogger(__name__)

# Override activitysim settings to read utility settings
@inject.injectable(override=True)
def settings():
    settings_dict = config.read_settings_file(utilityconfig.config_file(), mandatory=True)

    return settings_dict

@inject.step()
def filter_trips_by_date(trips_extended, trips, calendar_dates):
    # removes trips that do not occur on specified date

    # filter calendars on date range
    date = int(config.setting('date'))
    dow = dow_from_date(date)

    # filter calendar_dates for relevant calendar exceptions
    calendar_dates_df = calendar_dates.to_frame()

    added_on_date = (calendar_dates_df['date'] == date) & (calendar_dates_df['exception_type'] == 1)
    services_added_on_date = calendar_dates_df[added_on_date]['service_id']

    removed_on_date = (calendar_dates_df['date'] == date) & (calendar_dates_df['exception_type'] == 2)
    services_removed_on_date = calendar_dates_df[removed_on_date]['service_id']

    # filter trips and write to table
    trips_df = trips.to_frame()
    trips_columns = trips_df.columns

    date_in_range = (trips_extended['start_date'] <= date) & (date <= trips_extended['end_date'])
    dow_in_service = trips_extended[dow] == 1
    service_added_on_date = trips_extended['service_id'].isin(services_added_on_date)
    service_removed_on_date = trips_extended['service_id'].isin(services_removed_on_date)

    trips_filter = (date_in_range & dow_in_service & ~service_removed_on_date) | service_added_on_date
    trips_filtered_df = trips_extended[trips_filter]

    pipeline.replace_table("trips", trips_filtered_df[trips_columns])


def seconds_since_zero(military):
    # assert time_is_valid_format(military), 'Time format invalid for %s, please use HH:MM:SS', military

    t = military.split(':')
    hours = int(t[0])
    minutes = int(t[1])
    seconds = int(t[2])

    return hours * 3600 + minutes * 60 + seconds

def time_range_in_range(start_time_a, end_time_a, start_time_b, end_time_b):
    return (start_time_b <= start_time_a) & (end_time_a <= end_time_b)

def make_time_ranges_df(time_ranges):
    # Returns dataframe of time_ranges with columns start_time, end_time

    start_times = []
    end_times = []

    for time_range in time_ranges:
        start_times.append(time_range['start'])
        end_times.append(time_range['end'])

    return pd.DataFrame({ 'start_time': start_times, 'end_time': end_times})


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

def get_long_form_unwrapped_frequencies_inrange_df(unwrapped_repeating_trips):
    time_ranges = config.setting('time_ranges')
    time_ranges_df = make_time_ranges_df(time_ranges) \
        .rename(columns={ 'start_time': 'range_start', 'end_time': 'range_end' })
    
    # create long-form dataframe with row for each (trip_id, trip_order, time_range)
    range_index_array = time_ranges_df.index.array
    
    unwrapped_repeating_trips['range_index'] = None
    unwrapped_repeating_trips['range_index'] = unwrapped_repeating_trips['range_index'].apply(lambda x: np.array(range_index_array))

    unwrapped_repeating_trips = unwrapped_repeating_trips.explode('range_index')

    # HACK I'm not sure why explode is producing duplicates here (1 row explodes to 64 rows instead of 2), but we can remove them here
    unwrapped_repeating_trips = unwrapped_repeating_trips.drop_duplicates()

    # determine if in ranges
    unwrapped_frequencies_with_ranges_df = unwrapped_repeating_trips.merge(time_ranges_df, left_on='range_index', right_index=True)
    kwargs = {'in_range' : lambda x: time_range_in_range( \
        x['trip_start'].transform(seconds_since_zero), \
        x['trip_end'].transform(seconds_since_zero), \
        x['range_start'].transform(seconds_since_zero), \
        x['range_end'].transform(seconds_since_zero)) \
    }

    return unwrapped_frequencies_with_ranges_df.assign(**kwargs)

@inject.step()
def filter_repeating_trips_by_time(trips, unwrapped_repeating_trips, frequencies, stop_times):
    # edit start_time and end_time of frequencies partially in range (at least one but not all trips occur in range)
    # edit stop_times for trip if start_time has changed

    # do nothing if no repeating trips
    if (unwrapped_repeating_trips.empty):
        return

    unwrapped_long = get_long_form_unwrapped_frequencies_inrange_df(unwrapped_repeating_trips)

    # trips_in_range = unwrapped_long.groupby(['frequency_start', 'trip_id', 'trip_order'])['in_range'] \
    #     .any() \
    #     .rename(columns={'in_range'})
    # unwrapped_long = unwrapped_long.merge(trips_in_range, on=['frequency_start', 'trip_id', 'trip_order'])

    unwrapped_grouped = unwrapped_long.groupby(['frequency_start', 'trip_id'])

    # Remove frequencies with no trips in range
    any_trip_in_frequency_in_range_series = unwrapped_grouped['in_range'].any() \
        .rename('any_frequency_trip_in_range')
    unwrapped_long = unwrapped_long \
        .merge(any_trip_in_frequency_in_range_series.to_frame().reset_index(), on=['frequency_start', 'trip_id'])
    unwrapped_long = unwrapped_long[unwrapped_long['any_frequency_trip_in_range'] == True] \
        .drop('any_frequency_trip_in_range', axis='columns')

    # Remove trip from trips.txt if trip_id not in any range in frequencies
    trips_not_in_any_range_series = unwrapped_long.groupby(['trip_id'])['in_range'].any()
    trips_not_in_any_range_series = trips_not_in_any_range_series[trips_not_in_any_range_series == False]

    trips_df = trips.to_frame().reset_index()
    trips_filtered_df = trips_df[~trips_df['trip_id'].isin(trips_not_in_any_range_series.index.to_series())]
    
    # Shorten and/or push back frequencies if needed
    unwrapped_grouped = unwrapped_long.groupby(['frequency_start', 'trip_id'])
    unwrapped_grouped_last_trip = unwrapped_grouped['trip_order'].max().rename('last_trip_order')

    unwrapped_in_range_only_grouped = unwrapped_long[unwrapped_long['in_range'] == True].groupby(['frequency_start', 'trip_id'])
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

    unwrapped_long = unwrapped_long.merge(unwrapped_grouped_last_trip, left_on=['frequency_start', 'trip_id'], right_on=['frequency_start', 'trip_id'])
    unwrapped_long = unwrapped_long.merge(unwrapped_grouped_first_trip_in_range, left_on=['frequency_start', 'trip_id'], right_on=['frequency_start', 'trip_id'])
    unwrapped_long = unwrapped_long.merge(unwrapped_grouped_last_trip_in_range, left_on=['frequency_start', 'trip_id'], right_on=['frequency_start', 'trip_id'])

    unwrapped_long.loc[unwrapped_long['last_trip_order'] > unwrapped_long['last_trip_order_in_range'], \
        'frequency_end'] = unwrapped_long['last_trip_end_in_range']
    
    unwrapped_long.loc[unwrapped_long['first_trip_order_in_range'] > 0, \
        'frequency_start'] = unwrapped_long['first_trip_start_in_range']

    unwrapped_long = unwrapped_long[ \
        (unwrapped_long['trip_order'] <= unwrapped_long['last_trip_order_in_range']) \
        & (unwrapped_long['trip_order'] >= unwrapped_long['first_trip_order_in_range']) \
    ]

    # split frequencies on gaps

    # agg individual in_range status
    unwrapped_trips_out_of_range = unwrapped_long[unwrapped_long['in_range'] == False]

    # copy over index columns we need to iteratively update
    unwrapped_long = unwrapped_long \
        .reset_index() \
        .set_index(['frequency_start', 'trip_id', 'trip_order'], drop=False)
    unwrapped_long = unwrapped_long.rename(columns={ \
        'frequency_start': 'new_frequency_start', \
        'trip_order': 'new_trip_order' \
    })

    # Perform update for each out-of-range trip on adjacent in-range trips
    for index, current_row in unwrapped_trips_out_of_range.iterrows():
        cur_frequency_start = current_row['frequency_start']
        cur_trip_id = current_row['trip_id']
        cur_trip_order = current_row['trip_order']

        # if next trip in range
        if (unwrapped_long.loc[cur_frequency_start, cur_trip_id, cur_trip_order + 1]['in_range'] == True):
            # update frequency start for all future trips in frequency
            new_trip_start = unwrapped_long.loc[ \
                (unwrapped_long['new_frequency_start'] == cur_frequency_start) \
                    & (unwrapped_long['trip_id'] == cur_trip_id) \
                    & (unwrapped_long['new_trip_order'] == cur_trip_order + 1), \
            ]['trip_start'].tolist()[0]
            unwrapped_long.loc[ \
                (unwrapped_long['new_frequency_start'] == cur_frequency_start) \
                    & (unwrapped_long['trip_id'] == cur_trip_id) \
                    & (unwrapped_long['new_trip_order'] >= cur_trip_order), \
                'new_frequency_start' \
            ] = new_trip_start

            # update trip order for all future trips in frequency
            unwrapped_long['new_trip_order'] = unwrapped_long.apply(lambda unwrapped_long: \
                unwrapped_long['new_trip_order'] - (cur_trip_order + 1) if ( \
                    (unwrapped_long['new_frequency_start'] == cur_frequency_start) \
                    & (unwrapped_long['trip_id'] == cur_trip_id) \
                    & (unwrapped_long['new_trip_order'] >= cur_trip_order) \
                ) else unwrapped_long['new_trip_order'], \
            axis='columns')

        # if next trip in range
        if (unwrapped_long.loc[cur_frequency_start, cur_trip_id, cur_trip_order - 1]['in_range'] == True):
            # update frequency end for all previous trips in frequency
            new_trip_end = unwrapped_long.loc[ \
                (unwrapped_long['new_frequency_start'] == cur_frequency_start) \
                    & (unwrapped_long['trip_id'] == cur_trip_id) \
                    & (unwrapped_long['new_trip_order'] == cur_trip_order - 1), \
            ]['trip_end'].tolist()[0]
            unwrapped_long.loc[ \
                (unwrapped_long['new_frequency_start'] == cur_frequency_start) \
                    & (unwrapped_long['trip_id'] == cur_trip_id) \
                    & (unwrapped_long['new_trip_order'] <= cur_trip_order), \
                'frequency_end' \
            ] = new_trip_end

    # Now we can finally remove all out-of-range entries and reshape back into frequencies 
    unwrapped_long = unwrapped_long[unwrapped_long['in_range'] == True]
    unwrapped_long = unwrapped_long \
        .reset_index(drop=True) \
        .rename(columns={ 'new_frequency_start': 'start_time', 'frequency_end': 'end_time' })
    filtered_frequencies_df = unwrapped_long[ \
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


def get_inrange_df(df, start_col, end_col):
    # returns df with df.index, and an "inrange_{start}_{end}"" column
    # for each timerange

    time_ranges = config.setting('time_ranges')

    df_bounds = df[[start_col, end_col]]

    # is trip in any time range
    for time_range in time_ranges:
        start = time_range['start']
        end = time_range['end']

        time_range_key = 'inrange_' + start + '_' + end

        kwargs = {time_range_key : lambda x: time_range_in_range( \
            df_bounds[start_col].transform(seconds_since_zero), \
            df_bounds[end_col].transform(seconds_since_zero), \
            seconds_since_zero(start), \
            seconds_since_zero(end) \
        )}
        df_bounds = df_bounds.assign(**kwargs)

    inrange_df = df_bounds \
        .drop(start_col, axis=1) \
        .drop(end_col, axis=1)

    return inrange_df
    

@inject.step()
def filter_single_trips_by_time(trips_extended, trips, frequencies, stop_times):
    # filters trips by time ranges provided in config
    # trips will only be kept if they start and end within the time range
    # TODO DON'T remove trips in frequencies.txt! stop_times for those trips are irrelevant

    time_ranges = config.setting('time_ranges')

    # add range information
    trips_extended['inrange'] = get_inrange_df(trips_extended, 'start_time', 'end_time') \
        .any(axis=1)

    # filter trips and write to table
    trips_filtered_df = trips_extended[ \
        (trips_extended['inrange'] == True) | trips_extended['is_repeating'] == True]
    trips_columns = trips.to_frame().columns

    pipeline.replace_table("trips", trips_filtered_df[trips_columns])

@inject.step()
def calculate_average_headways(trips_extended, stop_times, unwrapped_repeating_trips, agency, routes, output_dir):
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

    trips_extended = trips_extended.reset_index()

    frequency_trip_ids = unwrapped_repeating_trips['trip_id']

    stop_times_df = stop_times.to_frame()
    single_trip_starts = trips_extended[['trip_id', 'start_time']]
    
    # remove stop_times for trips in frequencies as they should be ignored
    single_trip_starts = single_trip_starts[~single_trip_starts['trip_id'].isin(frequency_trip_ids)]

    # unwrapped_repeating_trips['trip_id'] = unwrapped_repeating_trips['trip_id'] + '_' + unwrapped_repeating_trips['trip_order'].astype(str)
    unwrapped_repeating_trips = unwrapped_repeating_trips[['trip_id', 'trip_start']]
    unwrapped_repeating_trips = unwrapped_repeating_trips.rename(columns={ 'trip_start': 'start_time' })

    trip_start_times = pd.concat([single_trip_starts, unwrapped_repeating_trips])

    trip_start_times = trip_start_times.merge(trips_extended.reset_index()[['trip_id','route_id', 'direction_id']], how='left', on='trip_id')
    trip_start_times['start_time_seconds'] = trip_start_times['start_time'].transform(seconds_since_zero)

    # calculate deltas
    trip_start_times.sort_values(['route_id', 'direction_id', 'trip_id', 'start_time_seconds'], inplace=True)
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
