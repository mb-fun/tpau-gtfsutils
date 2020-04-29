import logging
from activitysim.core import inject
from activitysim.core import config
from src.core.gtfsprocessor import GTFSProcessor

logger = logging.getLogger(__name__)

gtfs_processor = None

@inject.step()
def initialize_gtfs_tables():
    global gtfs_processor
    gtfs_filename = config.setting('gtfs_filename')
    gtfs_processor = GTFSProcessor(gtfs_filename)
    gtfs_processor.initialize_gtfs_feed()

@inject.step()
def cleanup_data_dir():
    global gtfs_processor
    gtfs_processor.cleanup_gtfs_files_in_data_dir()

@inject.injectable()
def gtfs_processor():
    return gtfs_processor