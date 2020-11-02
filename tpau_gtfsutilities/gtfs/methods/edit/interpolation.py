import numpy as np
from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.helpers.datetimehelpers import seconds_since_zero, seconds_to_military

def interpolate_stop_times():
    # returns false if interpolation not possible

    stop_times = gtfs.get_table('stop_times')
    shapes = gtfs.get_table('shapes')

    no_shape_dist_traveled = 'shape_dist_traveled' not in stop_times.columns \
        or stop_times['shape_dist_traveled'].isna().all()

    no_shapes_txt = not gtfs.has_table('shapes') or shapes.empty

    if (no_shape_dist_traveled or no_shapes_txt):
        return False

    # build table with chunk information

    df = stop_times.copy()
    df['has_arrival'] = df['arrival_time'].notna()
    df['has_departure'] = df['departure_time'].notna()

    df = df[df['has_arrival'] | df['has_departure']]
    timepoints_only = df[df['has_arrival'] | df['has_departure']]

    # https://stackoverflow.com/questions/50411098/how-to-do-forward-rolling-sum-in-pandas
    df['next_stop_sequence'] = timepoints_only.sort_values(by=['trip_id', 'stop_sequence']) \
        .iloc[::-1] \
        .groupby('trip_id')['stop_sequence'].transform(lambda x: x.rolling(2).max()) \
        .iloc[::-1] \

    # cleanup
    df['next_stop_sequence'] = df['next_stop_sequence'].fillna(df['stop_sequence']).astype('int64')

    df['stop_sequence_list'] = df.apply(lambda row: \
        list(range(row['stop_sequence'], row['next_stop_sequence']) \
        if row['stop_sequence'] != row['next_stop_sequence'] \
        else [row['stop_sequence']] \
    ), axis=1)

    df = df.explode('stop_sequence_list')
    df = df.rename(columns={'stop_sequence': 'start_seq', 'next_stop_sequence': 'end_seq', 'stop_sequence_list': 'stop_sequence'})

    chunks = df.set_index(['trip_id', 'stop_sequence']) \
        [['start_seq', 'end_seq']]


    stop_times = stop_times.set_index(['trip_id', 'stop_sequence'])
    stop_times = stop_times.merge(chunks, \
        how='left',
        right_index=True,
        left_index=True,
    )

    start_time = stop_times['departure_time'].rename('start_time')
    end_time = stop_times['arrival_time'].rename('end_time')
    start_sdt = stop_times['shape_dist_traveled'].rename('start_sdt')
    end_sdt = stop_times['shape_dist_traveled'].rename('end_sdt')

    stop_times = stop_times.merge(start_time, \
        left_on=['trip_id', 'start_seq'],
        right_index=True
    )
    
    stop_times = stop_times.merge(end_time, \
        left_on=['trip_id', 'end_seq'],
        right_index=True
    )
    
    stop_times = stop_times.merge(start_sdt, \
        left_on=['trip_id', 'start_seq'],
        right_index=True
    )
    
    stop_times = stop_times.merge(end_sdt, \
        left_on=['trip_id', 'end_seq'],
        right_index=True
    )

    # print('Annie F 11-02-2020 stop_times.dtypes: %s', stop_times.dtypes)
    # print('Annie F 11-02-2020 stop_times[start_time]: %s', stop_times['start_time'])
    # print('Annie F 11-02-2020 stop_times[start_time]: %s', stop_times['end_time'])
    # print('Annie F 11-02-2020 stop_times[start_time].transform(seconds_since_zero): %s', stop_times['start_time'].transform(seconds_since_zero).dtype)
    # print('Annie F 11-02-2020 stop_times[end_time].transform(seconds_since_zero): %s', stop_times['end_time'].transform(seconds_since_zero).dtype)

    def interpolate_row(row):
        # happens if last stop or on 1-stop chunks (consecutive timepoints)
        if (row['start_time'] == row['end_time']):
            return row['start_time']

        return seconds_to_military( \
            seconds_since_zero(row['start_time']) + \
                int(round( \
                    ( \
                        (row['shape_dist_traveled'] - row['start_sdt']) / (row['end_sdt'] - row['start_sdt']) \
                    ) * ( \
                        seconds_since_zero(row['end_time']) - seconds_since_zero(row['start_time']) \
                    ) \
                ))
            )

    stop_times['interp'] = stop_times.apply(lambda row: interpolate_row(row), axis=1)
    stop_times['arrival_time'] = stop_times['arrival_time'].fillna(stop_times['interp'])
    stop_times['departure_time'] = stop_times['departure_time'].fillna(stop_times['interp'])

    gtfs.update_table('stop_times', stop_times.reset_index())

    return True