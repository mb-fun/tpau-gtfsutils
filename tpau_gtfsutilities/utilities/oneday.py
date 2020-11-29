from .gtfsutility import GTFSUtility
from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.gtfs.gtfsreader import GTFSReader

from tpau_gtfsutilities.config.utilityconfig import utilityconfig

from tpau_gtfsutilities.gtfs.methods.edit.calendars import remove_exception_calendars
from tpau_gtfsutilities.gtfs.methods.filters.subset import subset_entire_feed

class OneDay(GTFSUtility):
    name = 'one_day'

    def run(self):
        settings = utilityconfig.get_settings()

        for feed in settings['gtfs_feeds']:
            print("Processing " + feed + "...")
            gtfsreader = GTFSReader(feed)
            gtfs.load_feed(gtfsreader)
            gtfs.preprocess()

            remove_exception_calendars()
            subset_entire_feed(settings['date_range'], settings['time_range'])
            feed_no_extension = feed[:-4]
            gtfs.write_feed(feed_no_extension)
