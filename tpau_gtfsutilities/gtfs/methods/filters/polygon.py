import geopandas as gpd

from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs

def filter_stops_by_multipolygon(multipolygon):
    # Note: this does not clean up the gtfs after removing stops
    # multipolygon is a shapely MultiPolygon

    stops = gtfs.get_table('stops')

    stops_gdf = gpd.GeoDataFrame(stops, geometry=gpd.points_from_xy(stops['stop_lon'], stops['stop_lat']), crs='EPSG:4326')

    stops_in_area = stops_gdf.geometry.transform(lambda g: multipolygon.contains(g)).rename('in_area')

    stops = stops.merge(stops_in_area.to_frame(), \
        how='left', \
        left_index=True, \
        right_index=True \
    )

    stops_filtered = stops[stops['in_area']]

    gtfs.update_table('stops', stops_filtered)

    