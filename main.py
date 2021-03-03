import logging
import os
import argparse
import yaml

from tpau_gtfsutilities.config.utilityconfig import utilityconfig
from tpau_gtfsutilities.config.utilityoutput import utilityoutput
from tpau_gtfsutilities.utilities.utility_manager import UtilityManager

def run():
    parser = argparse.ArgumentParser()
    utilitymanager = UtilityManager()
    valid_utilities = utilitymanager.get_utilities()

    utility_help = 'Utility name. Must be one of: ' + '\n'.join(valid_utilities)
    input_help = 'Input directory (defaults to data/)'
    output_help = 'Output directory (defaults to output/)'
    config_help = 'Yaml config file. If not provided, it will look for a matching file in the configs/ directory (i.e. one_day.yaml)'
    continue_on_error_help = 'Continue on error. If a common error is thrown before all feeds have been processed, utilities will proceed with the rest of the feeds. Mostly useful for testing.'

    parser.add_argument('-u', '--utility', help=utility_help, required=True, choices=valid_utilities, nargs='?')
    parser.add_argument('-i', '--input-dir', help=input_help, required=False, nargs='?')
    parser.add_argument('-c', '--config', help=config_help, required=False, nargs='?')
    parser.add_argument('-o', '--output-dir', help=output_help, required=False, nargs='?')
    parser.add_argument('-e', '--continue-on-error', help=continue_on_error_help, action='store_true')
    
    args = parser.parse_args()
    utility = args.utility
    input_dir = args.input_dir
    output_dir = args.output_dir
    config = args.config
    continue_on_error = args.continue_on_error

    utilityconfig.set_utility(utility)
    if input_dir:
        utilityconfig.set_input_dir(input_dir)
    if config:
        utilityconfig.set_config_file(config)
    if output_dir:
        utilityoutput.set_parent_output_dir(output_dir)

    utilityoutput.initialize_utility(utility)

    utilityrunner = utilitymanager.get_utility(utility)
    utilityrunner.run(continue_on_error=continue_on_error)


if __name__ == '__main__':
    run()
