from .gtfsutility import GTFSUtility
from tpau_gtfsutilities.config.utilityconfig import utilityconfig
from tpau_gtfsutilities.config.utilityoutput import utilityoutput
from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.gtfs.gtfscollectionsingleton import gtfs_collection
from tpau_gtfsutilities.gtfs.gtfsreader import GTFSReader

from tpau_gtfsutilities.gtfs.methods.edit.calendars import remove_exception_calendars
from tpau_gtfsutilities.gtfs.methods.edit.cluster import cluster_stops
from tpau_gtfsutilities.gtfs.methods.analysis.stopvisits import calculate_stop_visits
from tpau_gtfsutilities.gtfs.methods.filters.subset import subset_entire_feed

class ClusterStops(GTFSUtility):
    name = 'cluster_stops'

    def run(self, continue_on_error=False):
        # because this function aggregates results, continue_on_error is unused
        # and utility is ran collectively on feeds rather than one-by-one

        settings = utilityconfig.get_settings()

        for feed in settings['gtfs_feeds']:
            self.load_feed_into_gtfs_singleton(feed)

            time_range_defined = 'time_range' in settings.keys() \
                and not isinstance(settings['time_range'], str) \
                and 'start' in settings['time_range'].keys() \
                and 'end' in settings['time_range'].keys()

            if time_range_defined:
                subset_entire_feed(settings['date_range'], settings['time_range'])
            else:
                subset_entire_feed(settings['date_range'])

            feed_no_extension = feed[:-4]
            gtfs_collection.add_feed(gtfs.copy(), feed_no_extension)

        cluster_stops(settings['cluster_radius'])

        # reset original data so stop_visits reports with new stops
        for feedname in gtfs_collection.feeds.keys():
            gtfsfeed = gtfs_collection.feeds[feedname]
            gtfsfeed.reset_original_tables()
        
        # write stop visits report with clustered stops
        combined_visits_report = gtfs_collection.get_combined_computed_table(lambda gtfs: calculate_stop_visits(gtfs_override=gtfs))
        
        utilityoutput.write_or_append_to_output_csv(combined_visits_report, 'stop_visit_report.csv')

        gtfs_collection.write_all_feeds()

        utilityoutput.write_metadata(settings)

