import logging
from activitysim.core import inject
from activitysim.core.input import read_input_table

logger = logging.getLogger(__name__)

@inject.table()
def board_alight():
    return get_registered_dataframe_for_table('board_alight')

@inject.table()
def trip_capacity():
    return get_registered_dataframe_for_table('trip_capacity')

@inject.table()
def rider_trip():
    return get_registered_dataframe_for_table('rider_trip')

@inject.table()
def ridership():
    return get_registered_dataframe_for_table('ridership')

@inject.table()
def ride_feed_info():
    return get_registered_dataframe_for_table('ride_feed_info')
