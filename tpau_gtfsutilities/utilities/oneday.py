from .gtfsutility import GTFSUtility
from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.gtfs.gtfsreader import GTFSReader

from tpau_gtfsutilities.config.utilityconfig import utilityconfig
from tpau_gtfsutilities.config.utilityoutput import utilityoutput

from tpau_gtfsutilities.gtfs.methods.edit.calendars import remove_exception_calendars
from tpau_gtfsutilities.gtfs.methods.filters.subset import subset_entire_feed

class OneDay(GTFSUtility):
    name = 'one_day'
    write_feed = True

    def run_on_gtfs_singleton(self, settings):
        remove_exception_calendars()
        subset_entire_feed(settings['date_range'], settings['time_range'])
