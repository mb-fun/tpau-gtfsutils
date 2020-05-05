from .gtfsutility import GTFSUtility
from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.config.utilityconfig import utilityconfig

from tpau_gtfsutilities.gtfs.methods.filters.date import filter_trips_by_date

class AverageHeadway(GTFSUtility):
    name = 'average_headway'

    def run(self):
        settings = utilityconfig.get_settings()

        # TODO: valdate settings
        
        for feed in settings['gtfs_feeds']:
            gtfs.load_feed(feed)

            filter_trips_by_date(settings['date'])

            # for timerange in settings['time_ranges']:
                # filter_single_trips_by_time(settings['time'])
                # filter_repeating_trips_by_time(settings['time'])
                # prune_unused_trips_everywhere()
                # calculate_average_headways()

            gtfs.close_tables()
