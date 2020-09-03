import yaml
import os

class _UtilityConfig:
    # simple class for interfacing between abm injectables and main.py
    utility = None
    current_feed = None
    current_time_range = None # Needed for average_headway utility

    def set_utility(self, utility):
        # utiltiy is one of:
        #   average_headways
        #   one_day

        self.utility = utility

    def config_file(self):
        # config file is always {utilityname}.yaml
        return self.utility + '.yaml'

    def get_settings(self):
        config_path = os.path.join(self.configs_dir(), self.config_file())
        return yaml.load(open(config_path), Loader=yaml.BaseLoader)

    def input_dir(self):
        return 'data'
    
    def configs_dir(self):
        return 'configs'

    def set_current_feed(self, feed):
        self.current_feed = feed

    def get_current_feed(self):
        return self.current_feed
    
    def set_current_time_range(self, time_range):
        self.current_time_range = time_range

    def get_current_time_range(self):
        return self.current_time_range

utilityconfig = _UtilityConfig()