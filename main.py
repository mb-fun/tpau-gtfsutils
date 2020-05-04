# ActivitySim
# See full license in LICENSE.txt.

from __future__ import print_function

# import sys
# if not sys.warnoptions:  # noqa: E402
#     import warnings
#     warnings.filterwarnings('error', category=Warning)
#     warnings.filterwarnings('ignore', category=PendingDeprecationWarning, module='future')
#     warnings.filterwarnings('ignore', category=FutureWarning, module='pandas')

import logging
import os
import argparse
import getopt
import yaml

from src import abm
from src.core.utilityconfig import utilityconfig
from src.core.utilityoutput import utilityoutput

from activitysim.core import tracing
from activitysim.core import config
from activitysim.core.config import setting
from activitysim.core import pipeline

valid_utilities = [ \
    'average_headway', \
]

def process_args():
    parser = argparse.ArgumentParser()
    utility_help = 'Utility name. Must be one of: ' + '\n'.join(valid_utilities)
    parser.add_argument('-u', '--utility', help=utility_help, required=True, choices=valid_utilities, nargs='?')
    
    args = parser.parse_args()
    utilityconfig.set_utility(args.utility)
    utilityoutput.initialize(args.utility)

    # activitysim process args
    config.handle_standard_args(parser)

def run_pipeline():

    # If you provide a resume_after argument to pipeline.run
    # the pipeline manager will attempt to load checkpointed tables from the checkpoint store
    # and resume pipeline processing on the next submodel step after the specified checkpoint
    resume_after = setting('resume_after', None)

    if resume_after:
        print("resume_after", resume_after)

    pipeline.run(models=setting('models'), resume_after=resume_after)
    pipeline.close_pipeline()

def run():

    process_args()

    # specify None for a pseudo random base seed
    # inject.add_injectable('rng_base_seed', 0)

    tracing.config_logger()
    config.filter_warnings()

    tracing.delete_csv_files()

    config_path = os.path.join(config.configs_dir(), utilityconfig.config_file())
    config_yaml = yaml.load(open(config_path), Loader=yaml.BaseLoader)

    for gtfs_file in config_yaml['gtfs_files']:
        utilityconfig.set_current_feed(gtfs_file)

        if 'time_ranges' in config_yaml.keys():
            for time_range in config_yaml['time_ranges']:
                utilityconfig.set_current_time_range(time_range)
                run_pipeline()

        else:
            if 'time_range' in config_yaml.keys():
                utilityconfig.set_current_time_range(config_yaml['time_range'])
            run_pipeline()


if __name__ == '__main__':
    run()
