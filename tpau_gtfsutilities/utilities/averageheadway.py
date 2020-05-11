from .gtfsutility import GTFSUtility
from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.config.utilityconfig import utilityconfig

from tpau_gtfsutilities.gtfs.methods.filters.date import filter_trips_by_date
from tpau_gtfsutilities.gtfs.methods.filters.timerange import filter_single_trips_by_time
from tpau_gtfsutilities.gtfs.methods.filters.timerange import filter_repeating_trips_by_time
from tpau_gtfsutilities.gtfs.methods.filters.prune import prune_unused_trips_everywhere
from tpau_gtfsutilities.gtfs.methods.analysis.averageheadways import calculate_average_headways

class AverageHeadway(GTFSUtility):
    name = 'average_headway'

    def run(self):
        settings = utilityconfig.get_settings()

        # TODO: valdate settings
        
        for feed in settings['gtfs_feeds']:
            gtfs.load_feed(feed)

            filter_trips_by_date(settings['date'])

            for timerange in settings['time_ranges']:
                filter_single_trips_by_time(timerange)
                filter_repeating_trips_by_time(timerange)
                prune_unused_trips_everywhere()
                calculate_average_headways(settings['date'], timerange)

            gtfs.close_tables()
