from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.gtfs.methods.filters.daterange import filter_calendars_by_daterange, filter_calendar_dates_by_daterange
from tpau_gtfsutilities.gtfs.methods.filters.timerange import filter_single_trips_by_timerange, filter_repeating_trips_by_timerange
import tpau_gtfsutilities.gtfs.methods.filters.prune as prune

def subset_entire_feed(daterange, timerange=None):
    filter_calendars_by_daterange(daterange)
    filter_calendar_dates_by_daterange(daterange)
    filter_single_trips_by_timerange(timerange)
    filter_repeating_trips_by_timerange(timerange)
    prune.prune_unused_trips()
    prune.prune_unused_calendars()
    prune.prune_unused_stops()
    prune.prune_unused_routes()
    prune.prune_unused_shapes()
    # reset_feed_dates(daterange)