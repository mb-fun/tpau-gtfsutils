import os

from tpau_gtfsutilities.config.utilityconfig import utilityconfig
from tpau_gtfsutilities.config.utilityoutput import utilityoutput
from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.gtfs.gtfsreader import GTFSReader
from tpau_gtfsutilities.gtfs.methods.filters import daterange
from tpau_gtfsutilities.helpers.datetimehelpers import GTFSDateRange
from tpau_gtfsutilities.helpers.datetimehelpers import GTFSDate

# Base class for utilities
class GTFSUtility:
    name = None
    write_feed = False # If set to True, will output feed after utility runs

    def run_on_gtfs_singleton(self, settings):
        # Feed-level utility operations
        # In most cases, all utility operations go here
        pass
    
    def run(self, continue_on_error=False):
        # Runs the utilities with the loaded configuration
        # continue_on_error will continue running on rest of feeds if
        #   a common error (ValueError, KeyError, TypeError, FileNotFoundError) is encountered
        #   and is most useful for testing on multiple feeds at once

        settings = utilityconfig.get_settings()

        for feed in settings['gtfs_feeds']:
            print("Processing " + feed + "...")

            if continue_on_error:
                try:
                    self.load_and_run_on_feed(feed, settings)
                # catch common errors so utils can continue running on other feeds
                except (ValueError, KeyError, TypeError, FileNotFoundError, ZeroDivisionError) as e:
                    print("ERROR: ", e)
            else:
                self.load_and_run_on_feed(feed, settings)

        utilityoutput.write_metadata(settings)

    def load_and_run_on_feed(self, feed, settings):
        self.load_feed_into_gtfs_singleton(feed)
        self.warn_if_any_input_dates_outside_gtfs_singleton_bounds(settings)
        self.configure_output(feed)
        self.run_on_gtfs_singleton(settings)
        if self.write_feed:
            feed_no_extension = feed[:-4]
            gtfs.write_feed(feed_no_extension)

    def load_feed_into_gtfs_singleton(self, feed):
        # loads feed from config into singleton to be used by util
        gtfsreader = GTFSReader(feed)
        gtfs.load_feed(gtfsreader)
        gtfs.preprocess()

    def configure_output(self, feed):
        # TODO: This functionality should be moved elsewhere
        feed_no_extension = feed[:-4]
        utilityoutput.set_feedname(feed_no_extension)

    def warn_if_date_not_within_gtfs_calendar_bounds(self, input_date):
        calendar_dr = daterange.get_feed_calendar_service_daterange()
        if not calendar_dr.includes(input_date):
            print("WARNING: no service found for this date in calendar.txt: ")
            print("     Input date: " + input_date.datestring())
            print("     Min/max calendar bounds : (" + calendar_dr.start.datestring() + ", " + calendar_dr.end.datestring() + ")")
    
    def warn_if_date_not_within_feed_bounds(self, input_date):
        feed_dr = daterange.get_feed_start_end_daterange()
        if feed_dr is None: return

        if not feed_dr.includes(input_date):
            print("WARNING: Input date outside of feed start/end dates: ")
            print("     Input date: " + input_date.datestring())
            print("     Feed start/end (as found in feed_info.txt) : (" + feed_dr.start.datestring() + ", " + feed_dr.end.datestring() + ")")
    
    def warn_if_daterange_not_within_gtfs_calendar_bounds(self, input_dr):
        calendar_dr = daterange.get_feed_calendar_service_daterange()

        if not calendar_dr.includes_daterange(input_dr):
            print("WARNING: daterange includes dates not within feed's calendar bounds: ")
            print("     Input daterange: (" + input_dr.start.datestring() + ", " + input_dr.end.datestring() + ")")
            print("     Min/max calendar bounds : (" + calendar_dr.start.datestring() + ", " + calendar_dr.end.datestring() + ")")

    def warn_if_daterange_not_within_feed_bounds(self, input_dr):
        feed_dr = daterange.get_feed_start_end_daterange()
        if feed_dr is None: return

        if not feed_dr.includes_daterange(input_dr):
            print("WARNING: daterange includes dates not within feed start/end dates: ")
            print("     Input daterange: (" + input_dr.start.datestring() + ", " + input_dr.end.datestring() + ")")
            print("     Feed start/end (as found in feed_info.txt) : (" + feed_dr.start.datestring() + ", " + feed_dr.end.datestring() + ")")

    def warn_if_any_input_dates_outside_gtfs_singleton_bounds(self, settings):
        if 'date_range' in settings:
            input_dr = GTFSDateRange(settings['date_range']['start'], settings['date_range']['end'])
            self.warn_if_daterange_not_within_gtfs_calendar_bounds(input_dr)
            self.warn_if_daterange_not_within_feed_bounds(input_dr)

        if 'date' in settings:
            input_date = GTFSDate(settings['date'])
            self.warn_if_date_not_within_gtfs_calendar_bounds(input_date)
            self.warn_if_date_not_within_feed_bounds(input_date)