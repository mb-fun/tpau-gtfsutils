from .gtfsutility import GTFSUtility

from tpau_gtfsutilities.config.utilityoutput import utilityoutput
from tpau_gtfsutilities.gtfs.methods.filters.date import filter_trips_by_date
from tpau_gtfsutilities.gtfs.methods.filters.timerange import filter_single_trips_by_timerange
from tpau_gtfsutilities.gtfs.methods.filters.timerange import filter_repeating_trips_by_timerange
from tpau_gtfsutilities.gtfs.methods.analysis.averageheadways import calculate_average_headways
from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs

class AverageHeadway(GTFSUtility):
    name = 'average_headway'
    
    def run_on_gtfs_singleton(self, settings):
        output_file = 'average_headways.csv'

        time_ranges_defined = 'time_ranges' in settings.keys() \
            and not isinstance(settings['time_ranges'], str) \
            and len(settings['time_ranges']) \
            and 'start' in settings['time_ranges'][0].keys()

        filter_trips_by_date(settings['date'])
        if not time_ranges_defined:
            utilityoutput.write_or_append_to_output_csv(calculate_average_headways(settings['date'], None), output_file)
        else:
            gtfs.update_original_tables() # update after filtering by date for easier processing
            
            for timerange in settings['time_ranges']:
                filter_single_trips_by_timerange(timerange)
                filter_repeating_trips_by_timerange(timerange)
                utilityoutput.write_or_append_to_output_csv(calculate_average_headways(settings['date'], timerange), output_file, write_gtfs_filename=True)
                gtfs.reset_to_original_tables()
