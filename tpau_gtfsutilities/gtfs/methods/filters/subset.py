from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.gtfs.methods.filters.daterange import filter_calendars_by_daterange, filter_calendar_dates_by_daterange, filter_board_alight_by_daterange, reset_feed_dates
from tpau_gtfsutilities.gtfs.methods.filters.timerange import filter_single_trips_by_timerange, filter_repeating_trips_by_timerange

def subset_entire_feed(daterange, timerange=None, trim_trips=False):
    filter_calendars_by_daterange(daterange)
    filter_calendar_dates_by_daterange(daterange)
    filter_board_alight_by_daterange(daterange)
    if timerange and timerange['start'] and timerange['end']:
        filter_single_trips_by_timerange(timerange, trim_trips=trim_trips)
        filter_repeating_trips_by_timerange(timerange, trim_trips=trim_trips)
    reset_feed_dates(daterange)