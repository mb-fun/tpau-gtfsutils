import os

from tpau_gtfsutilities.config.utilityconfig import utilityconfig
from tpau_gtfsutilities.config.utilityoutput import utilityoutput
from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.gtfs.gtfsreader import GTFSReader

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

    def load_and_run_on_feed(self, feed, settings):
        self.load_feed_into_gtfs_singleton(feed)
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