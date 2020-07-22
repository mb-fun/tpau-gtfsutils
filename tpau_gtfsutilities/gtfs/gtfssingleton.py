import pandas as pd
import os

from .gtfsreader import GTFSReader
from tpau_gtfsutilities.config.utilityconfig import utilityconfig
from tpau_gtfsutilities.config.utilityoutput import utilityoutput
from .properties import REQUIRED_TABLES
from .gtfserrors import MissingRequiredFileError

table_index = { \
    'agency': ['agency_id'], \
    'stops': ['stop_id'], \
    'routes': ['route_id'], \
    'trips': ['trip_id'], \
    'calendar': ['service_id'], \
    'fare_attributes': ['fare_id'], \
    'shapes': ['shape_id'] \
}

class _GTFSSingleton:
    _gtfsreader = None
    _tables = {} # collection of GTFS dataframes
    _original_tables = {} # collection of GTFS dataframes as inputted

    def load_feed(self, filename):
        self._gtfsreader = GTFSReader(filename)
        self._gtfsreader.unpack_csvs()
        self._initialize_tables()

    def _initialize_tables(self):
        for tablename in self._gtfsreader.contents:
            csv = os.path.join(utilityconfig.input_dir(), tablename + '.csv')
            df = pd.read_csv(csv)
            if (tablename in table_index.keys()):
                df = df.set_index(table_index[tablename])
            self._tables[tablename] = df
            self._original_tables[tablename] = df.copy()

    def write_feed(self, feedname):
        utilityoutput.write_to_zip(self._tables, feedname)

    def get_table(self, tablename, index=True, original=False):
        if tablename not in self._tables.keys():
            if tablename in REQUIRED_TABLES:
                raise MissingRequiredFileError(tablename)
            return pd.DataFrame()
        if original:
            tabledict = self._original_tables
        else:
            tabledict = self._tables

        if not index:
            return tabledict[tablename].reset_index()
        return tabledict[tablename]

    def has_table(self, tablename):
        return tablename in self._tables.keys()

    def get_columns(self, tablename, index=True):
        columns = self._gtfsreader.contents.get(tablename).copy()
        if (not index) and (tablename in table_index.keys()):
            columns = [col for col in columns if col not in table_index[tablename]]
        return columns

    def update_table(self, tablename, df, allow_column_changes=False):
        columns = self.get_columns(tablename, index=False)
        if allow_column_changes:
            columns_with_index = list(set(df.columns.tolist() + df.index.names))
            self._gtfsreader.update_table_columns(tablename, columns_with_index)
        
        # make sure index is correctly set
        if (tablename in table_index.keys()):
            index = table_index[tablename].copy()
            index.sort()
            df_index = df.index.names.copy()
            df_index.sort()
            if not index == df_index:
                df = df.set_index(index)

        self._tables[tablename] = df[columns]

    def is_gtfs_ride(self):
        # This is the only required file in the GTFS-Ride spec (5/4/2020)
        # https://github.com/ODOT-PTS/GTFS-ride/blob/master/spec/en/reference.md
        return self.has_table('ride_feed_info')

    def close_tables(self):
        self._gtfsreader.cleanup_gtfs_files_in_data_dir()

gtfs = _GTFSSingleton()