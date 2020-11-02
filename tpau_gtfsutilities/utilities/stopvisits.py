import geopandas as gpd
from shapely.geometry import MultiPolygon

from .gtfsutility import GTFSUtility
from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.config.utilityconfig import utilityconfig

from tpau_gtfsutilities.gtfs.methods.edit.calendars import remove_exception_calendars
from tpau_gtfsutilities.gtfs.methods.filters.subset import subset_entire_feed
from tpau_gtfsutilities.gtfs.methods.filters.polygon import filter_stops_by_multipolygon
from tpau_gtfsutilities.gtfs.methods.analysis.stopvisits import calculate_stop_visits

class StopVisits(GTFSUtility):
    name = 'stop_visits'

    def read_multipolygon_from_file(self, filepath):
        # Input: path to either shapefile or geojson
        # returns a multipolygon so both polygon and multipolygon
        # inputs can be read

        gdf = gpd.read_file(filepath).to_crs(epsg=4326)
        return MultiPolygon(gdf.geometry.iloc[0])

    def run(self):
        settings = utilityconfig.get_settings()

        for feed in settings['gtfs_feeds']:
            gtfs.load_feed(feed)

            subset_entire_feed(settings['date_range'], settings['time_range'])
            polygon_file = settings['polygon']
            
            if (polygon_file):
                polygon_file_path = self.input_file_path(polygon_file)
                multipolygon = self.read_multipolygon_from_file(polygon_file_path)
                filter_stops_by_multipolygon(multipolygon)

            calculate_stop_visits()
            gtfs.close_tables()
