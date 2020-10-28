import os

from tpau_gtfsutilities.config.utilityconfig import utilityconfig
from tpau_gtfsutilities.config.utilityoutput import utilityoutput

# Base class for utilities
class GTFSUtility:
    name = None

    def input_file_path(self, filename):
        return os.path.join(utilityconfig.input_dir(), filename)
    
    def run(self):
        pass