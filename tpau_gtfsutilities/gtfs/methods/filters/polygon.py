import geopandas as gpd
from shapely.geometry import Point, MultiPolygon

from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs

def filter_stops_by_multipolygon(geo_df):
    # Note: this does not clean up the gtfs after removing stops
    # geo_df is a geopandas df of polygons, can only contain one entry

    stops = gtfs.get_table('stops')
    geo_df = geo_df.to_crs(epsg=4326)

    polygon = MultiPolygon(geo_df.geometry.iloc[0])

    stops_gdf = gpd.GeoDataFrame(stops, geometry=gpd.points_from_xy(stops['stop_lon'], stops['stop_lat']))
    stops_gdf = stops_gdf.set_crs(epsg=4326)

    stops_in_area = stops_gdf.geometry.transform(lambda g: polygon.contains(g)).rename('in_area')

    stops = stops.merge(stops_in_area.to_frame(), \
        how='left', \
        left_index=True, \
        right_index=True \
    )

    stops_filtered = stops[stops['in_area']]

    gtfs.update_table('stops', stops_filtered)

    