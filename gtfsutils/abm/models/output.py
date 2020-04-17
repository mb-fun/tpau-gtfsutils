import logging

from activitysim.core import inject
# from activitysim.core.steps.output import write_data_dictionary
from activitysim.core.steps.output import write_tables

logger = logging.getLogger(__name__)


@inject.injectable(cache=True)
def preload_injectables():
    logger.info("preload_injectables")

    # inject.add_step('write_data_dictionary', write_data_dictionary)
    inject.add_step('write_tables', write_tables)
