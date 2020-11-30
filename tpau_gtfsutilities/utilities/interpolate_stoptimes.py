import geopandas as gpd

from .gtfsutility import GTFSUtility
from tpau_gtfsutilities.gtfs.methods.edit.interpolation import interpolate_stop_times

class InterpolateStoptimes(GTFSUtility):
    name = 'interpolate_stoptimes'
    write_feed = True

    def run_on_gtfs_singleton(self, settings):
        if not interpolate_stop_times():
            print("Cannot interpolate stop times -- feed needs to have shapes.txt and needs to use shape_dist_traveled in stop_times.txt")
