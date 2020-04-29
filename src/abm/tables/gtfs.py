import logging
import pandas as pd
from activitysim.core import inject
from activitysim.core import config
from activitysim.core.input import read_input_table

logger = logging.getLogger(__name__)

@inject.injectable()
def register_dataframe_for_table(tablename):
    print('Annie F 04-25-2020 register_dataframe_for_table: %s', tablename)
    df = read_input_table(tablename)

    # replace seems necessary for using a table twice in a model, i.e. for merged tables
    inject.add_table(tablename, df, replace=True)

    return df

def register_dataframe_for_optional_table(tablename, headers, gtfs_processor):
    # returns emtpy dataframe if table not present in input_table_list

    print('Annie F 04-28-2020 gtfs_processor: %s', gtfs_processor)
    tables = gtfs_processor.tables()
    df = read_input_table(tablename) if tablename in tables else pd.DataFrame(columns=headers)

    # replace seems necessary for using a table twice in a model, i.e. for merged tables
    inject.add_table(tablename, df, replace=True)
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
def calendar(gtfs_processor):
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

    return register_dataframe_for_optional_table('calendar', headers, gtfs_processor)

@inject.table()
def calendar_dates(gtfs_processor):
    headers = [
        'service_id',
        'date',
        'exception_type'
    ]
    df = register_dataframe_for_optional_table('calendar_dates', headers, gtfs_processor)
    return df

@inject.table()
def fare_attributes(gtfs_processor):
    headers = [
        'fare_id',
        'price',
        'currency_type',
        'payment_method',
        'transfers',
        'agency_id',
        'transfer_duration'	
    ]
    return register_dataframe_for_optional_table('fare_attributes', headers, gtfs_processor)

@inject.table()
def fare_rules(gtfs_processor):
    headers = [
        'fare_id',
        'route_id',
        'origin_id',
        'destination_id',
        'contains_id'
    ]
    return register_dataframe_for_optional_table('fare_rules', headers, gtfs_processor)

@inject.table()
def shapes(gtfs_processor):
    headers = [
        'shape_id',
        'shape_pt_lat',
        'shape_pt_lon',
        'shape_pt_sequence',
        'shape_dist_traveled'
    ]
    return register_dataframe_for_optional_table('shapes', headers, gtfs_processor)

@inject.table()
def frequencies(gtfs_processor):
    headers = [
        'trip_id',
        'start_time',
        'end_time',
        'headway_secs',
        'exact_times'

    ]
    return register_dataframe_for_optional_table('frequencies', headers, gtfs_processor)

@inject.table()
def transfers(gtfs_processor):
    headers = [
        'from_stop_id',
        'to_stop_id',
        'transfer_type',
        'min_transfer_time'
    ]
    return register_dataframe_for_optional_table('transfers', headers, gtfs_processor)

@inject.table()
def pathways(gtfs_processor):
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
    return register_dataframe_for_optional_table('pathways', headers, gtfs_processor)

@inject.table()
def levels(gtfs_processor):
    headers = [
        'level_id',
        'level_index',
        'level_name'
    ]
    return register_dataframe_for_optional_table('levels', headers, gtfs_processor)


@inject.table()
def translations(gtfs_processor):
    headers = [
        'table_name',
        'field_name',
        'language',
        'translation',
        'record_id',
        'record_sub_id',
        'field_value'
    ]
    return register_dataframe_for_optional_table('translations', headers, gtfs_processor)

@inject.table()
def feed_info(gtfs_processor):
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
    return register_dataframe_for_optional_table('feed_info', headers, gtfs_processor)

@inject.table()
def attributions(gtfs_processor):
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
    return register_dataframe_for_optional_table('attributions', headers, gtfs_processor)
