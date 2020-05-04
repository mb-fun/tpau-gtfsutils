import pandas as pd
import os

from activitysim.core import config

class _UtilityOutput:
    utility = None
    dir_index = 0

    def initialize(self, utility):
        # utiltiy is one of:
        #   average_headways

        self.utility = utility
        self.create_output_dir()

    def create_output_dir(self):
        while (os.path.exists(self.get_output_dir())):
            self.dir_index += 1
        os.mkdir(self.get_output_dir())

    def get_output_dir(self):
        dirname = self.utility + '_' + str(self.dir_index) if self.dir_index > 0 else self.utility
        return os.path.join(config.output_dir(), dirname)

    def write_or_append_to_output_csv(self, df, filename, index=False):
        csvfile = os.path.join(self.get_output_dir(), filename)
        if os.path.exists(csvfile):
            df.to_csv(csvfile, mode='a', header=False, index=index)
        else:
            df.to_csv(csvfile, index=index)

utilityoutput = _UtilityOutput()