class _UtilityConfig:
    # simple class for interfacing between abm injectables and main.py
    utility = None
    current_feed = None
    current_time_range = None # Needed for average_headway utility

    def set_utility(self, utility):
        # utiltiy is one of:
        #   average_headways

        self.utility = utility

    def config_file(self):
        # config file is always {utilityname}.yaml
        return self.utility + '.yaml'

    def set_current_feed(self, feed):
        self.current_feed = feed

    def get_current_feed(self):
        return self.current_feed
    
    def set_current_time_range(self, time_range):
        self.current_time_range = time_range

    def get_current_time_range(self):
        return self.current_time_range


utilityconfig = _UtilityConfig()