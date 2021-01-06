import numpy as np
from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.gtfs.gtfsenums import GTFSBool
from tpau_gtfsutilities.helpers.datetimehelpers import GTFSDateRange
from tpau_gtfsutilities.helpers.datetimehelpers import GTFSDate

def filter_calendars_by_daterange(daterange):

    calendar = gtfs.get_table('calendar')
    filter_daterange = GTFSDateRange(daterange['start'], daterange['end'])

    calendar['_gtfs_daterange'] = calendar.apply(lambda row: GTFSDateRange(row['start_date'], row['end_date']), axis=1)
    calendar['_overlap'] = calendar['_gtfs_daterange'].apply(lambda dr: \
        filter_daterange.overlap(dr) \
    )

    # we want to remove calendar entries that don't overlap DOWs 
    calendar['_dows_overlap'] = calendar.apply(lambda row: \
        GTFSBool.TRUE in (row[dow] for dow in filter_daterange.days_of_week()),
        axis=1
    )
    
    # we want to keep calendar entries that are used in overlapping exceptions 
    if gtfs.has_table('calendar_dates'):
        calendar_dates = gtfs.get_table('calendar_dates')
        calendar_dates['_date_overlap'] = calendar_dates.apply(lambda row: filter_daterange.includes(row['date']), axis=1)
        calendar_dates = calendar_dates[calendar_dates['_date_overlap']]
        calendar['_exception_overlap'] = calendar.index.to_series().isin(calendar_dates['service_id'])
    else:
        calendar['_exception_overlap'] = False

    calendar = calendar[(calendar['_overlap'].notnull() & calendar['_dows_overlap']) | calendar['_exception_overlap']]

    # trim bounds to fit within daterange
    calendar['start_date'] = calendar['_overlap'].apply(lambda dr: dr.start.datestring())
    calendar['end_date'] = calendar['_overlap'].apply(lambda dr: dr.end.datestring())

    gtfs.update_table('calendar', calendar)

def filter_calendar_dates_by_daterange(daterange):
    if not gtfs.has_table('calendar_dates'): return

    calendar_dates = gtfs.get_table('calendar_dates')
    filter_daterange = GTFSDateRange(daterange['start'], daterange['end'])

    calendar_dates['_gtfs_date'] = calendar_dates.apply(lambda row: GTFSDate(row['date']), axis=1)
    calendar_dates['_inrange'] = calendar_dates.apply(lambda row: filter_daterange.includes(row['date']), axis=1)

    calendar_dates_filtered = calendar_dates[calendar_dates['_inrange']]

    gtfs.update_table('calendar_dates', calendar_dates_filtered)

def remove_trips_with_nonexistent_calendars():
    calendar = gtfs.get_table('calendar', index=False)
    
    trips = gtfs.get_table('trips')
    trips_filtered = trips[trips['service_id'].isin(calendar['service_id'])]

    if (gtfs.has_table('frequencies')):
        frequencies = gtfs.get_table('frequencies')
        frequencies_filtered = frequencies[frequencies['trip_id'].isin(trips_filtered.index.to_series())]
        gtfs.update_table('frequencies', frequencies_filtered)

    gtfs.update_table('trips', trips_filtered)

def filter_board_alight_by_daterange(daterange):
    if not gtfs.has_table('board_alight'): return

    board_alight = gtfs.get_table('board_alight', index=False)
    if 'service_date' not in board_alight.columns: return

    filter_daterange = GTFSDateRange(daterange['start'], daterange['end'])

    board_alight['_inrange'] = board_alight.apply(lambda row: filter_daterange.includes(row['service_date']), axis=1)
    board_alight_filtered = board_alight[board_alight['_inrange']]

    gtfs.update_table('board_alight', board_alight_filtered)

def reset_feed_dates(daterange):
    if not gtfs.has_table('feed_info'): return

    gtfs_daterange = GTFSDateRange(daterange['start'], daterange['end'])
    feed_info = gtfs.get_table('feed_info')

    feed_info['feed_start_date'] = gtfs_daterange.start.datestring()
    feed_info['feed_end_date'] = gtfs_daterange.end.datestring()

    gtfs.update_table('feed_info', feed_info)
