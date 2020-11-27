import logging
import os
import argparse
import yaml

from tpau_gtfsutilities.config.utilityconfig import utilityconfig
from tpau_gtfsutilities.config.utilityoutput import utilityoutput
from tpau_gtfsutilities.utilities.averageheadway import AverageHeadway
from tpau_gtfsutilities.utilities.oneday import OneDay
from tpau_gtfsutilities.utilities.stopvisits import StopVisits
from tpau_gtfsutilities.utilities.interpolate_stoptimes import InterpolateStoptimes
from tpau_gtfsutilities.utilities.cluster_stops import ClusterStops

def get_utility_runner(utility):
    utilityrunner = None
    if utility == 'average_headway':
        return AverageHeadway()
    if utility == 'one_day':
        return OneDay()
    if utility == 'stop_visits':
        return StopVisits()
    if utility == 'interpolate_stoptimes':
        return InterpolateStoptimes()
    if utility == 'cluster_stops':
        return ClusterStops()

def run():
    parser = argparse.ArgumentParser()

    valid_utilities = [ \
        'average_headway', \
        'one_day', \
        'stop_visits', \
        'interpolate_stoptimes', \
        'cluster_stops', \
    ]
    utility_help = 'Utility name. Must be one of: ' + '\n'.join(valid_utilities)
    input_help = 'Input directory (defaults to data/)'
    output_help = 'Output directory (defaults to output/)'
    config_help = 'Yaml config file. If not provided, it will look for a matching file in the configs/ directory (i.e. one_day.yaml)'

    parser.add_argument('-u', '--utility', help=utility_help, required=True, choices=valid_utilities, nargs='?')
    parser.add_argument('-i', '--input-dir', help=input_help, required=False, nargs='?')
    parser.add_argument('-c', '--config', help=config_help, required=False, nargs='?')
    parser.add_argument('-o', '--output-dir', help=output_help, required=False, nargs='?')
    
    args = parser.parse_args()
    utility = args.utility
    input_dir = args.input_dir
    output_dir = args.output_dir
    config = args.config

    utilityconfig.set_utility(utility)
    if input_dir:
        utilityconfig.set_input_dir(input_dir)
    if config:
        utilityconfig.set_config_file(config)
    if output_dir:
        utilityoutput.set_parent_output_dir(output_dir)

    utilityoutput.initialize_utility(utility)

    utilityrunner = get_utility_runner(utility)
    utilityrunner.run()


if __name__ == '__main__':
    run()
