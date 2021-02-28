from .gtfsutility import GTFSUtility

from tpau_gtfsutilities.config.utilityoutput import utilityoutput
from tpau_gtfsutilities.gtfs.methods.filters.date import filter_trips_by_date
from tpau_gtfsutilities.gtfs.methods.filters.timerange import filter_single_trips_by_timerange
from tpau_gtfsutilities.gtfs.methods.filters.timerange import filter_repeating_trips_by_timerange
from tpau_gtfsutilities.gtfs.methods.analysis.averageheadways import calculate_average_headways

class AverageHeadway(GTFSUtility):
    name = 'average_headway'

    def run_on_gtfs_singleton(self, settings):

        time_ranges_defined = 'time_ranges' in settings.keys() \
            and not isinstance(settings['time_ranges'], str) \
            and len(settings['time_ranges']) \
            and 'start' in settings['time_ranges'][0].keys()

        if not time_ranges_defined:
            filter_trips_by_date(settings['date'])
            utilityoutput.write_or_append_to_output_csv(calculate_average_headways(settings['date'], None))
        else:
            for timerange in settings['time_ranges']:
                filter_trips_by_date(settings['date'])
                filter_single_trips_by_timerange(timerange)
                filter_repeating_trips_by_timerange(timerange)
                utilityoutput.write_or_append_to_output_csv(calculate_average_headways(settings['date'], timerange))
