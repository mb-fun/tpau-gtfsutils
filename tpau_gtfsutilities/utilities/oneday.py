from .gtfsutility import GTFSUtility
from tpau_gtfsutilities.gtfs.methods.edit.calendars import remove_exception_calendars
from tpau_gtfsutilities.gtfs.methods.filters.subset import subset_entire_feed

class OneDay(GTFSUtility):
    name = 'one_day'
    write_feed = True

    def run_on_gtfs_singleton(self, settings):
        remove_exception_calendars()

        time_range_defined = 'time_range' in settings.keys() \
            and not isinstance(settings['time_range'], str) \
            and 'start' in settings['time_range'].keys() \
            and 'end' in settings['time_range'].keys()

        if time_range_defined:
            subset_entire_feed(settings['date_range'], settings['time_range'])
        else:
            subset_entire_feed(settings['date_range'])
