import pandas as pd
import os

from .gtfsreader import GTFSReader
from tpau_gtfsutilities.config.utilityconfig import utilityconfig

table_indeces = { \
    'agency': 'agency_id', \
    'stops': 'stop_id', \
    'routes': 'route_id', \
    'trips': 'trip_id', \
    'calendar': 'service_id', \
    'fare_attributes': 'fare_id', \
    'shapes': 'shape_id' \
}

class _GTFSSingleton:
    _gtfsreader = None
    _tables = {} # collection of GTFS dataframes

    def load_feed(self, filename):
        self._gtfsreader = GTFSReader(filename)
        self._gtfsreader.unpack_csvs()
        self._initialize_tables()

    def _initialize_tables(self):
        for tablename in self._gtfsreader.contents:
            csv = os.path.join(utilityconfig.input_dir(), tablename + '.csv')
            df = pd.read_csv(csv)
            if (tablename in table_indeces.keys()):
                df = df.set_index(table_indeces[tablename])
            self._tables[tablename] = df

    def get_table(self, tablename, index=True):
        if tablename not in self._tables.keys():
            return None
        if not index:
            return self._tables[tablename].reset_index()
        return self._tables[tablename]

    def get_columns(self, tablename, index=True):
        columns = self._gtfsreader.contents.get(tablename).copy()
        if (not index) and (tablename in table_indeces.keys()):
            columns.remove(table_indeces[tablename])
        return columns

    def update_table(self, tablename, df):
        # TODO if column changes update _gtfsreader contents
        self._tables[tablename] = df

    def has_table(self, tablename):
        return (tablename in  self._tables.keys())

    def is_gtfs_ride(self):
        # This is the only required file in the GTFS-Ride spec (5/4/2020)
        # https://github.com/ODOT-PTS/GTFS-ride/blob/master/spec/en/reference.md
        return self.has_table('ride_feed_info')

    def close_tables(self):
        self._gtfsreader.cleanup_gtfs_files_in_data_dir()

gtfs = _GTFSSingleton()