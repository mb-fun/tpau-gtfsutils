from .gtfsutility import GTFSUtility
from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.gtfs.gtfsreader import GTFSReader
from tpau_gtfsutilities.config.utilityconfig import utilityconfig
from tpau_gtfsutilities.config.utilityoutput import utilityoutput

from tpau_gtfsutilities.gtfs.methods.filters.date import filter_trips_by_date
from tpau_gtfsutilities.gtfs.methods.filters.timerange import filter_single_trips_by_timerange
from tpau_gtfsutilities.gtfs.methods.filters.timerange import filter_repeating_trips_by_timerange
from tpau_gtfsutilities.gtfs.methods.filters.prune import prune_unused_trips
from tpau_gtfsutilities.gtfs.methods.analysis.averageheadways import calculate_average_headways

class AverageHeadway(GTFSUtility):
    name = 'average_headway'

    def run(self):
        settings = utilityconfig.get_settings()

        for feed in settings['gtfs_feeds']:
            feed_no_extension = feed[:-4]
            utilityoutput.set_feedname(feed_no_extension)
            print("Processing " + feed + "...")
            gtfsreader = GTFSReader(feed)
            gtfs.load_feed(gtfsreader)
            gtfs.preprocess()
            
            time_ranges_defined = settings['time_ranges'] and len(settings['time_ranges']) and settings['time_ranges'][0]['start']
            if not time_ranges_defined:
                filter_trips_by_date(settings['date'])
                prune_unused_trips()
                calculate_average_headways(settings['date'], None)
            else:
                for timerange in settings['time_ranges']:
                    filter_trips_by_date(settings['date'])
                    filter_single_trips_by_timerange(timerange)
                    filter_repeating_trips_by_timerange(timerange)
                    prune_unused_trips()
                    calculate_average_headways(settings['date'], timerange)
