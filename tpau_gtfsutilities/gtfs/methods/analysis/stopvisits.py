import pandas as pd
import numpy as np

from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.config.utilityoutput import utilityoutput
from tpau_gtfsutilities.gtfs.methods.helpers import triphelpers
from tpau_gtfsutilities.gtfs.methods.helpers.calendarhelpers import GTFSServiceCalendar

def calculate_stop_visits():
    # For each stop, comma separated values, CR/LF for new stop/row
    #     Agency ID
    #     Agency Name
    #     Stop ID
    #     Stop Name
    #     Stop Latitude
    #     Stop Longitude
    #     Trip(s) Served (scheduled visits to stop)
    #     Visit Count [for specified time period]
    #     Board-count [if GTFS-ride input, for specified time period]
    #     Alight-count [if GTFS-ride input, for specified time period, if available]


    # use original so report includes stops that have been filtered by polygon or date/time
    stops = gtfs.get_table('stops', original=True)[['stop_name', 'stop_lat', 'stop_lon']]

    # stop visits
    stop_trip_counts = get_stop_trip_counts()

    # number of trips that serve stop
    num_trips = get_stop_trip_counts(service_counts=False)

    #TODO: build report output

    return


def get_stop_trip_counts(service_counts=True):
    # If service_counts is false, returns a series with the number of trips each stop occurs on
    # If service_counts is true, returns a series with the number of times a stop is served for all of the feed's service

    unwrapped_repeating_trips = triphelpers.get_unwrapped_repeating_trips()

    trip_scheduled_stops = gtfs.get_table('stop_times')[['trip_id', 'stop_id', 'arrival_time', 'departure_time']]
    repeating_trip_counts = unwrapped_repeating_trips['trip_id'].value_counts().rename('trip_counts')

    trip_stop_pairs = trip_scheduled_stops[['trip_id', 'stop_id']]
    if not service_counts:
        trip_stop_pairs = trip_stop_pairs.drop_duplicates()

    trip_stop_pairs = trip_stop_pairs.merge( \
        repeating_trip_counts.to_frame(), \
        how='left', \
        left_on='trip_id', \
        right_index=True \
    ).fillna(1)

    if not service_counts:
        return trip_stop_pairs[['stop_id', 'trip_counts']].groupby(['stop_id']).sum()

    trip_service_counts = get_trip_service_counts()
    
    trip_stop_pairs_with_service = trip_stop_pairs.merge( \
        trip_service_counts.rename('active_days').to_frame(), \
        how='left', \
        left_on='trip_id', \
        right_index=True \
    )

    trip_stop_pairs_with_service['service_trips'] = \
        trip_stop_pairs_with_service['trip_counts'] * trip_stop_pairs_with_service['active_days']
    
    stop_service_counts = trip_stop_pairs_with_service[['stop_id', 'service_trips']].groupby(['stop_id']).sum()

    return stop_service_counts.rename(columns={'service_trips':'trip_counts'})
    


def get_trip_service_counts():
    # Returns a series with the number of service days each trip has

    trips_extended = triphelpers.get_trips_extended()
    calendar = gtfs.get_table('calendar').reset_index()

    calendar_active_days = calendar['service_id'].to_frame().reset_index()
    
    calendar_active_days['active_days'] = calendar_active_days['service_id'].apply(lambda id: \
        GTFSServiceCalendar(id).num_active_days() \
    )

    # https://stackoverflow.com/questions/11976503/how-to-keep-index-when-using-pandas-merge
    trips_extended = trips_extended.reset_index().merge( \
        calendar_active_days, \
        how='left', \
        on='service_id' \
    ).set_index('trip_id').drop_duplicates()

    return trips_extended['active_days']