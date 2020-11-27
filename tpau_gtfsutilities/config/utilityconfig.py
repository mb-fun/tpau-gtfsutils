import yaml
import os

class _UtilityConfig:
    # simple class for interfacing between abm injectables and main.py
    utility = None
    current_feed = None
    current_time_range = None # Needed for average_headway utility
    config_file = None
    input_dir = None

    def set_utility(self, utility):
        # utiltiy is one of:
        #   average_headways
        #   one_day
        #   cluster_stops
        #   interpolate_stoptimes
        #   stop_visits

        self.utility = utility

    def set_config_file(self, cf):
        self.config_file = cf
    
    def set_input_dir(self, dir):
        self.input_dir = dir

    def get_settings(self):
        return yaml.load(open(self.get_config_file()), Loader=yaml.BaseLoader)

    def get_input_dir(self):
        default_input_dir = 'data'
        dir = self.input_dir if self.input_dir else default_input_dir
        return dir
    
    def get_config_file(self):
        default_cf = 'configs/' + self.utility + 'yaml'
        cf = self.config_file if self.config_file else default_cf
        return cf

    def set_current_feed(self, feed):
        self.current_feed = feed

    def get_current_feed(self):
        return self.current_feed
    
    def set_current_time_range(self, time_range):
        self.current_time_range = time_range

    def get_current_time_range(self):
        return self.current_time_range

utilityconfig = _UtilityConfig()