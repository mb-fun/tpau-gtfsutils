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
    trips = gtfs.get_table('trips', original=True)

    if not ('direction_id' in trips.columns):
        trips['direction_id'] = ''
        trips_extended['direction_id'] = ''

    route_direction_pairs = trips[['route_id', 'direction_id']].drop_duplicates()
    route_direction_pairs = route_direction_pairs.set_index(['route_id', 'direction_id'])

    agency_info = gtfs.get_table('agency')[['agency_id','agency_name']]

    if 'agency_id' in gtfs.get_columns('routes'):
        route_info = gtfs.get_table('routes')[['agency_id', 'route_long_name']]

        output = route_direction_pairs.reset_index() \
            .merge(route_info, how='left', on='route_id') \
            .set_index(['route_id', 'direction_id'])
        output = output.reset_index() \
            .merge(agency_info, how='left', on='agency_id') \
            .set_index(['route_id', 'direction_id'])

    # No agency id in routes.txt means there is only one agency
    else:
        route_info = gtfs.get_table('routes')['route_long_name']
        output = route_direction_pairs.reset_index() \
            .merge(route_info.to_frame(), how='left', on='route_id') \
            .set_index(['route_id', 'direction_id'])
        
        output['agency_id'] = agency_info['agency_id'].iloc[0]
        output['agency_name'] = agency_info['agency_name'].iloc[0]

    output['date'] = date
    output['start_time'] = time_range['start'] if time_range else ''
    output['end_time'] = time_range['end'] if time_range else ''

    if trips_extended.empty:
        output['trip_start_times'] = np.empty((len(output), 0)).tolist()
        output['average_headway_mintes'] = 0

        return output.reset_index()

    unwrapped_repeating_trips = triphelpers.get_unwrapped_repeating_trips()

    trip_start_times = trips_extended[['trip_id', 'start_time']]

    if not unwrapped_repeating_trips.empty:
        frequency_trip_ids = unwrapped_repeating_trips['trip_id']

        # remove stop_times for trips in frequencies as they should be ignored
        trip_start_times = trip_start_times[~trip_start_times['trip_id'].isin(frequency_trip_ids)]

        unwrapped_repeating_trips = unwrapped_repeating_trips[['trip_id', 'trip_start']]
        unwrapped_repeating_trips = unwrapped_repeating_trips.rename(columns={ 'trip_start': 'start_time' })

        trip_start_times = pd.concat([trip_start_times, unwrapped_repeating_trips])

    trip_start_times = trip_start_times.merge(trips_extended.reset_index()[['trip_id','route_id', 'direction_id']], how='left', on='trip_id')
    trip_start_times['start_time_seconds'] = trip_start_times['start_time'].transform(seconds_since_zero)

    # calculate deltas
    trip_start_times.sort_values(['route_id', 'direction_id', 'start_time_seconds'], inplace=True)
    trip_start_times['delta_seconds'] = trip_start_times['start_time_seconds'].diff()
    first_trip_in_route_dir = (trip_start_times['route_id'] != trip_start_times['route_id'].shift(1)) \
        | ( \
            (trip_start_times['direction_id'] != '') \
            & (trip_start_times['direction_id'] != trip_start_times['direction_id'].shift(1)) \
        )

    trip_start_times.loc[first_trip_in_route_dir, 'delta_seconds'] = np.nan

    route_avg_headway_minutes = trip_start_times \
        .groupby(['route_id', 'direction_id'])['delta_seconds'].mean() \
        .fillna(0) \
        .transform(lambda x: np.round(x / 60, decimals=3)) \
        .rename('average_headway_minutes')

    route_trip_starts_list = trip_start_times.groupby(['route_id', 'direction_id'])['start_time'].apply(list) \
        .rename('trip_start_times')

    output['average_headway_minutes'] = route_avg_headway_minutes
    output = output.merge(route_trip_starts_list, how='left', left_index=True, right_index=True)

    # fill empty trip start times with empty list
    output['trip_start_times'] = output['trip_start_times'].apply(lambda d: d if isinstance(d, list) else [])

    return output.reset_index()

