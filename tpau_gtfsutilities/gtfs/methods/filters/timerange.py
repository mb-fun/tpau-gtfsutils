from tpau_gtfsutilities.gtfs.methods.helpers import triphelpers
from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs

from tpau_gtfsutilities.helpers.datetimehelpers import seconds_since_zero

def time_range_in_range(start_time_a, end_time_a, start_time_b, end_time_b):
    return (start_time_b <= start_time_a) & (end_time_a <= end_time_b)

def get_long_form_unwrapped_frequencies_inrange_df(time_range):

    unwrapped_repeating_trips = triphelpers.get_unwrapped_repeating_trips()
    if unwrapped_repeating_trips.empty:
        return pd.DataFrame()
    
    unwrapped_repeating_trips['range_start'] = time_range['start']
    unwrapped_repeating_trips['range_end'] = time_range['end']

    kwargs = {'in_range' : lambda x: time_range_in_range( \
        x['trip_start'].transform(seconds_since_zero), \
        x['trip_end'].transform(seconds_since_zero), \
        x['range_start'].transform(seconds_since_zero), \
        x['range_end'].transform(seconds_since_zero)) \
    }

    return unwrapped_repeating_trips.assign(**kwargs)

def filter_repeating_trips_by_time(time_range):
    # edit start_time and end_time of frequencies partially in range (at least one but not all trips occur in range)
    # edit stop_times for trip if start_time has changed

    unwrapped_long = get_long_form_unwrapped_frequencies_inrange_df(time_range)

    # do nothing if no repeating trips
    if (unwrapped_long.empty):
        return

    unwrapped_grouped = unwrapped_long.groupby(['frequency_start', 'trip_id'])

    # Remove frequencies with no trips in range
    any_trip_in_frequency_in_range_series = unwrapped_grouped['in_range'].any() \
            .rename('any_frequency_trip_in_range')
    unwrapped_long = unwrapped_long \
        .merge(any_trip_in_frequency_in_range_series.to_frame().reset_index(), on=['frequency_start', 'trip_id'])
    unwrapped_long = unwrapped_long[unwrapped_long['any_frequency_trip_in_range'] == True] \
        .drop('any_frequency_trip_in_range', axis='columns')

    # Remove trip from trips.txt if trip_id not in any range in frequencies
    trips_not_in_any_range = unwrapped_long.groupby(['trip_id'])['in_range'].any()
    trips_not_in_any_range = trips_not_in_any_range[trips_not_in_any_range == False]

    trips_df = gtfs.get_table('trips', index=False)
    trips_filtered_df = trips_df[~trips_df['trip_id'].isin(trips_not_in_any_range.index.to_series())]
    
    # Shorten and/or push back frequencies if needed
    unwrapped_grouped = unwrapped_long.groupby(['frequency_start', 'trip_id'])
    last_trip_order = unwrapped_grouped['trip_order'].max().rename('last_trip_order')

    unwrapped_in_range_only_grouped = unwrapped_long[unwrapped_long['in_range'] == True].groupby(['frequency_start', 'trip_id'])

    # TODO: handle if unwrapped_in_range_only_grouped is empty here (all frequencies out of range), causes error

    last_trip_order_in_range = unwrapped_in_range_only_grouped.apply(lambda g: g[g['trip_order'] == g['trip_order'].max()]) \
        [['frequency_start', 'trip_id', 'trip_order', 'trip_end']]
    last_trip_order_in_range = last_trip_order_in_range \
        .rename(columns={ 'trip_order': 'last_trip_order_in_range', 'trip_end': 'last_trip_end_in_range' }) \
        .reset_index(drop=True)
    
    first_trip_order_in_range = unwrapped_in_range_only_grouped.apply(lambda g: g[g['trip_order'] == g['trip_order'].min()]) \
        [['frequency_start', 'trip_id', 'trip_order', 'trip_start']]
    first_trip_order_in_range = first_trip_order_in_range \
        .rename(columns={ 'trip_order': 'first_trip_order_in_range', 'trip_start': 'first_trip_start_in_range' }) \
        .reset_index(drop=True)

    unwrapped_long = unwrapped_long.merge(last_trip_order, left_on=['frequency_start', 'trip_id'], right_on=['frequency_start', 'trip_id'])
    unwrapped_long = unwrapped_long.merge(first_trip_order_in_range, left_on=['frequency_start', 'trip_id'], right_on=['frequency_start', 'trip_id'])
    unwrapped_long = unwrapped_long.merge(last_trip_order_in_range, left_on=['frequency_start', 'trip_id'], right_on=['frequency_start', 'trip_id'])

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

        # if previous trip in range
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
    filtered_frequencies_df = unwrapped_long[gtfs.get_columns('frequencies')] \
        .drop_duplicates()

    gtfs.update_table('trips', trips_filtered_df.set_index('trip_id'))
    gtfs.update_table('frequencies', filtered_frequencies_df)


def get_inrange(df, start_col, end_col, time_range):
    # returns df with df.index, and an "inrange" column

    df_bounds = df[[start_col, end_col]]

    start = time_range['start']
    end = time_range['end']

    kwargs = {'inrange' : lambda x: time_range_in_range( \
        df_bounds[start_col].transform(seconds_since_zero), \
        df_bounds[end_col].transform(seconds_since_zero), \
        seconds_since_zero(start), \
        seconds_since_zero(end) \
    )}
    df_bounds = df_bounds.assign(**kwargs)

    inrange = df_bounds['inrange']

    return inrange
    

def filter_single_trips_by_time(timerange):
    # filters trips by time ranges provided in config
    # trips will only be kept if they start and end within the time range

    trips_extended = triphelpers.get_trips_extended()

    # add range information
    trips_extended['inrange'] = get_inrange(trips_extended, 'start_time', 'end_time', timerange)

    # filter trips and write to table
    trips_filtered_df = trips_extended[ \
        (trips_extended['inrange'] == True) | trips_extended['is_repeating'] == True]
    trips_columns = gtfs.get_columns('trips', index=False)

    gtfs.update_table('trips', trips_filtered_df[trips_columns])