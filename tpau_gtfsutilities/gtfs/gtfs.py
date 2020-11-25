import pandas as pd
import os

from .gtfsreader import GTFSReader
from tpau_gtfsutilities.config.utilityconfig import utilityconfig
from tpau_gtfsutilities.config.utilityoutput import utilityoutput
from .properties import REQUIRED_TABLES
from .gtfserrors import MissingRequiredFileError

table_index = { \
    'stops': ['stop_id'], \
    'routes': ['route_id'], \
    'trips': ['trip_id'], \
    'calendar': ['service_id'], \
    'fare_attributes': ['fare_id'], \
    'shapes': ['shape_id'] \
}

class GTFS:
    _gtfsreader = None
    _tables = {} # collection of GTFS dataframes
    _original_tables = {} # collection of GTFS dataframes as inputted

    def load_feed(self, gtfsreader):
        gtfsreader.unpack_csvs()

        for tablename in gtfsreader.contents:
            csv = os.path.join(utilityconfig.input_dir(), tablename + '.csv')
            df = pd.read_csv(csv)
            if (tablename in table_index.keys()):
                df = df.set_index(table_index[tablename])
            self._tables[tablename] = df
            self._original_tables[tablename] = df.copy()

        gtfsreader.cleanup_gtfs_files_in_data_dir()

    def write_feed(self, feedname):
        unindexed_tables = {}
        for tablename in self._tables.keys():
            has_index = tablename in table_index.keys()
            if has_index:
                unindexed_tables[tablename] = self._tables[tablename].reset_index()
            else:
                unindexed_tables[tablename] = self._tables[tablename]
        utilityoutput.write_to_zip(unindexed_tables, feedname)

    def get_table(self, tablename, index=True, original=False):
        if tablename not in self._tables.keys():
            if tablename in REQUIRED_TABLES:
                raise MissingRequiredFileError(tablename)
            return pd.DataFrame()
        if original:
            tabledict = self._original_tables
        else:
            tabledict = self._tables

        table = tabledict[tablename]
        if not index:
            table = table.reset_index()

        return table.copy()

    def has_table(self, tablename):
        return tablename in self._tables.keys()

    def table_has_column(self, tablename, column):
        return column in self.get_columns(tablename)

    def get_columns(self, tablename, index=True):
        if (not self.has_table(tablename)):
            return []
        columns = self._tables[tablename].columns.tolist()
        if (not index) and (tablename in table_index.keys()):
            columns = [col for col in columns if col not in table_index[tablename]]
        return columns

    def update_table(self, tablename, df, allow_column_changes=False):
        columns = self.get_columns(tablename, index=False)
        if allow_column_changes:
            columns_with_index = list(set(df.columns.tolist() + df.index.names))
            # self._gtfsreader.update_table_columns(tablename, columns_with_index)
        
        # make sure index is correctly set
        if (tablename in table_index.keys()):
            index = table_index[tablename].copy()
            index.sort()
            df_index = df.index.names.copy()
            df_index.sort()
            if not index == df_index:
                df = df.set_index(index)

        self._tables[tablename] = df[columns]

    def clear_table(self, tablename):
        df = self.get_table(tablename)
        emptied_df = df.iloc[0:0]

        self.update_table(tablename, emptied_df)

    def is_gtfs_ride(self):
        # This is the only required file in the GTFS-Ride spec (5/4/2020)
        # https://github.com/ODOT-PTS/GTFS-ride/blob/master/spec/en/reference.md
        return self.has_table('ride_feed_info')

    def is_multiagency(self):
        return self.get_table('agency')['agency_name'].size > 1

    def only_uses_calendar_dates(self):
        # Returns true if the feed defines all service using calendar_dates
        return self.has_table('calendar')

    # def close_tables(self):
    #     self._gtfsreader.cleanup_gtfs_files_in_data_dir()

    def copy(self):
        c = GTFS()
        c._tables = get_df_dict_copy(self._tables)
        c._original_tables = get_df_dict_copy(self._original_tables)
        return c


def get_df_dict_copy(dict):
    # helper for copy
    copy = {}
    for key in dict.keys():
        copy[key] = dict[key].copy()

    return copy