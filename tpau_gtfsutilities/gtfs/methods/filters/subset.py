from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.gtfs.methods.filters.daterange import filter_calendars_by_daterange

def subset_entire_feed(daterange, timerange=None):
    # - Remove calendars outside of date range
    # - prune calendars everywhere
    # - filter trips by time range
    # - prune trips everwhere
    # - prune stops (from stoptimes)
    # - prune routes (from trips)
    # - prune shapes (from trips)
    # - (probably more pruning)
    # - reset feed_info dates

    filter_calendars_by_daterange(daterange)