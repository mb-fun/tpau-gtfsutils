from tpau_gtfsutilities.utilities.averageheadway import AverageHeadway
from tpau_gtfsutilities.utilities.oneday import OneDay
from tpau_gtfsutilities.utilities.stopvisits import StopVisits
from tpau_gtfsutilities.utilities.interpolate_stoptimes import InterpolateStoptimes
from tpau_gtfsutilities.utilities.cluster_stops import ClusterStops

class UtilityManager:
    # Add utilities here
    utilities = {
        'average_headway': AverageHeadway(),
        'one_day': OneDay(),
        'stop_visits': StopVisits(),
        'interpolate_stoptimes': InterpolateStoptimes(),
        'cluster_stops': ClusterStops(),
    }

    def get_utilities(self):
        return self.utilities.keys()

    def get_utility(self, utility):
        return self.utilities[utility]

    def is_valid_utility(self, utility):
        return utility in self.get_utilities()