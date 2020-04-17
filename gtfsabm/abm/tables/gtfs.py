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
    return register_dataframe_for_table('calendar')

@inject.table()
def calendar_dates():
    df = register_dataframe_for_table('calendar_dates')
    return df

@inject.table()
def fare_attributes():
    return register_dataframe_for_table('fare_attributes')

@inject.table()
def fare_rules():
    return register_dataframe_for_table('fare_rules')

@inject.table()
def shapes():
    return register_dataframe_for_table('shapes')

@inject.table()
def frequencies():
    return register_dataframe_for_table('frequencies')

@inject.table()
def transfers():
    return register_dataframe_for_table('transfers')

@inject.table()
def levels():
    return register_dataframe_for_table('levels')

@inject.table()
def feed_info():
    return register_dataframe_for_table('feed_info')

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
