import logging
import os
import argparse
import yaml

from tpau_gtfsutilities.config.utilityconfig import utilityconfig
from tpau_gtfsutilities.config.utilityoutput import utilityoutput
from tpau_gtfsutilities.utilities.averageheadway import AverageHeadway

def get_utility_runner(utility):
    utilityrunner = None
    if utility == 'average_headway':
        return AverageHeadway()

def run():
    parser = argparse.ArgumentParser()

    valid_utilities = [ \
        'average_headway', \
    ]
    utility_help = 'Utility name. Must be one of: ' + '\n'.join(valid_utilities)
    parser.add_argument('-u', '--utility', help=utility_help, required=True, choices=valid_utilities, nargs='?')
    
    args = parser.parse_args()
    utility = args.utility


    utilityconfig.set_utility(utility)
    utilityoutput.initialize_utility(utility)

    utilityrunner = get_utility_runner(utility)
    utilityrunner.run()


if __name__ == '__main__':
    run()
