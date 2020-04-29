class _UtilityConfig:
    # simple class for interfacing between abm injectables and main.py
    utility = None

    def set_utility(self, utility):
        # utiltiy is one of:
        #   average_headways

        self.utility = utility

    def config_file(self):
        # config file is always {utilityname}.yaml
        return self.utility + '.yaml'

utilityconfig = _UtilityConfig()