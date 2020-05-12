import pandas as pd
import numpy as np

from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.config.utilityoutput import utilityoutput
from tpau_gtfsutilities.gtfs.methods.helpers import triphelpers

from tpau_gtfsutilities.helpers.datetimehelpers import seconds_since_zero

def calculate_average_headways(date, time_range):
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

    trips_extended = triphelpers.get_trips_extended().reset_index()
    unwrapped_repeating_trips = triphelpers.get_unwrapped_repeating_trips()

    frequency_trip_ids = unwrapped_repeating_trips['trip_id']

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

    route_direction_pairs = gtfs.get_table('trips', original=True)[['route_id', 'direction_id']] \
        .drop_duplicates() \
        .set_index(['route_id', 'direction_id'])

    route_avg_headway_minutes = trip_start_times \
        .groupby(['route_id', 'direction_id'])['delta_seconds'].mean() \
        .fillna(0) \
        .transform(lambda x: np.round(x / 60, decimals=3)) \
        .rename('average_headway_minutes')

    route_trip_starts_list = trip_start_times.groupby(['route_id', 'direction_id'])['start_time'].apply(list) \
        .rename('trip_start_times')

    route_direction_pairs['average_headway_minutes'] = route_avg_headway_minutes \
        .fillna(0)
    route_avg_headway_data = route_direction_pairs.merge(route_trip_starts_list, how='left', left_index=True, right_index=True)

    # fill empty trip start times with empty list
    route_avg_headway_data['trip_start_times'] = route_avg_headway_data['trip_start_times'].apply(lambda d: d if isinstance(d, list) else [])


    route_avg_headway_data = route_avg_headway_data[['trip_start_times', 'average_headway_minutes']]
    route_avg_headway_data = route_avg_headway_data.reset_index()
    route_info = gtfs.get_table('routes')[['agency_id', 'route_long_name']]
    agency_info = gtfs.get_table('agency')['agency_name']
    route_avg_headway_data = route_avg_headway_data.merge(route_info, on='route_id')
    route_avg_headway_data = route_avg_headway_data.merge(agency_info.to_frame(), on='agency_id')

    route_avg_headway_data['date'] = date

    route_avg_headway_data['start_time'] = time_range['start'] if time_range else ''
    route_avg_headway_data['end_time'] = time_range['end'] if time_range else ''

    route_avg_headway_data = route_avg_headway_data.reset_index()

    utilityoutput.write_or_append_to_output_csv(route_avg_headway_data, 'average_headways.csv')

