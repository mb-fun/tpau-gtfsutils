import logging
import pandas as pd
from activitysim.core import inject
from activitysim.core import config
from activitysim.core.input import read_input_table

logger = logging.getLogger(__name__)

@inject.injectable()
def register_dataframe_for_table(tablename):
    df = read_input_table(tablename)

    inject.add_table(tablename, df)

    return df

@inject.injectable()
def register_dataframe_for_optional_table(tablename, headers):
    # returns emtpy dataframe if table not present in input_table_list

    table_list = config.setting('input_table_list')
    table_present = False
    for table in table_list:
        if table['tablename'] == tablename:
            table_present = True

    df = read_input_table(tablename) if table_present else pd.DataFrame(columns=headers)

    inject.add_table(tablename, df)
    return df

@inject.table()
def agency():
    return register_dataframe_for_table('agency')

@inject.table()
def stops():
    return register_dataframe_for_table('stops')

@inject.table()
def routes():
    return register_dataframe_for_table('routes')

@inject.table()
def trips():
    return register_dataframe_for_table('trips')

@inject.table()
def stop_times():
    return register_dataframe_for_table('stop_times')

@inject.table()
def calendar():
    headers = [
        'service_id',
        'monday',
        'tuesday',
        'wednesday',
        'thursday',
        'friday',
        'saturday',
        'sunday',
        'start_date',
        'end_date'
    ]

    return register_dataframe_for_optional_table('calendar', headers)

@inject.table()
def calendar_dates():
    headers = [
        'service_id',
        'date',
        'exception_type'
    ]
    df = register_dataframe_for_optional_table('calendar_dates', headers)
    return df

@inject.table()
def fare_attributes():
    headers = [
        'fare_id',
        'price',
        'currency_type',
        'payment_method',
        'transfers',
        'agency_id',
        'transfer_duration'	
    ]
    return register_dataframe_for_optional_table('fare_attributes', headers)

@inject.table()
def fare_rules():
    headers = [
        'fare_id',
        'route_id',
        'origin_id',
        'destination_id',
        'contains_id'
    ]
    return register_dataframe_for_optional_table('fare_rules', headers)

@inject.table()
def shapes():
    headers = [
        'shape_id',
        'shape_pt_lat',
        'shape_pt_lon',
        'shape_pt_sequence',
        'shape_dist_traveled'
    ]
    return register_dataframe_for_optional_table('shapes', headers)

@inject.table()
def frequencies():
    headers = [
        'trip_id',
        'start_time',
        'end_time',
        'headway_secs',
        'exact_times'

    ]
    return register_dataframe_for_optional_table('frequencies', headers)

@inject.table()
def transfers():
    headers = [
        'from_stop_id',
        'to_stop_id',
        'transfer_type',
        'min_transfer_time'
    ]
    return register_dataframe_for_optional_table('transfers', headers)

@inject.table()
def pathways():
    headers = [
        'pathway_id',
        'from_stop_id',
        'to_stop_id',
        'pathway_mode',
        'is_bidirectional',
        'length',
        'traversal_time',
        'stair_count',
        'max_slope',
        'min_width',
        'signposted_as',
        'reversed_signposted_as'
    ]
    return register_dataframe_for_optional_table('pathways', headers)

@inject.table()
def levels():
    headers = [
        'level_id',
        'level_index',
        'level_name'
    ]
    return register_dataframe_for_optional_table('levels', headers)


@inject.table()
def translations():
    headers = [
        'table_name',
        'field_name',
        'language',
        'translation',
        'record_id',
        'record_sub_id',
        'field_value'
    ]
    return register_dataframe_for_optional_table('translations', headers)

@inject.table()
def feed_info():
    headers = [
        'feed_publisher_name',
        'feed_publisher_url',
        'feed_lang',
        'default_lang',
        'feed_start_date',
        'feed_end_date',
        'feed_version',
        'feed_contact_email',
        'feed_contact_url'
    ]
    return register_dataframe_for_optional_table('feed_info', headers)

@inject.table()
def attributions():
    headers = [
        'attribution_id',
        'agency_id',
        'route_id',
        'trip_id',
        'organization_name',
        'is_producer',
        'is_operator',
        'is_authority',
        'attribution_url',
        'attribution_email',
        'attribution_phone'
    ]
    return register_dataframe_for_optional_table('attributions', headers)
