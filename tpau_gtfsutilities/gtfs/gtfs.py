import pandas as pd
import numpy as np
import os

from .gtfsreader import GTFSReader
from tpau_gtfsutilities.config.utilityconfig import utilityconfig
from tpau_gtfsutilities.config.utilityoutput import utilityoutput
from tpau_gtfsutilities.gtfs.process import preprocess
from .properties import REQUIRED_TABLES, TABLE_INDECES, NUMERIC_DTYPES, DOWS
from .gtfserrors import MissingRequiredFileError
from .tables.calendar_dates import CalendarDates
from .tables.calendar import Calendar
from .tables.fare_attributes import FareAttributes
from .tables.routes import Routes
from .tables.shapes import Shapes
from .tables.stops import Stops
from .tables.stop_times import StopTimes
from .tables.trips import Trips
from .tables.gtfstable import GTFSTable

table_classes = {
    'calendar_dates': CalendarDates,
    'calendar': Calendar,
    'fare_attributes': FareAttributes,
    'routes': Routes,
    'shapes': Shapes,
    'stops': Stops,
    'stop_times': StopTimes,
    'trips': Trips
}

class GTFS:
    _gtfsreader = None
    _tables = {} # collection of GTFSTables

    def load_feed(self, gtfsreader):
        gtfsreader.unpack_csvs()

        def get_dtypes_dict(columns):
            dtypes = {}
            for col in columns:
                if col in NUMERIC_DTYPES.keys():
                    dtypes[col] = NUMERIC_DTYPES[col]
                else:
                    dtypes[col] = 'str'
            return dtypes

        for tablename in gtfsreader.contents:
            csv = os.path.join(utilityconfig.get_input_dir(), tablename + '.csv')
            columns = gtfsreader.contents[tablename]

            table_class = table_classes[tablename] \
                if tablename in table_classes.keys() \
                else GTFSTable

            self._tables[tablename] = table_class(csv, dtype=get_dtypes_dict(columns))

        gtfsreader.cleanup_gtfs_files_in_data_dir()

    def preprocess(self):
        preprocess.remove_all_wrapping_quotations_in_gtfs(self)

    def write_feed(self, feedname):
        tables = {}
        for tablename in self._tables.keys():
            table = self.get_table(tablename, index=False) 
            tables[tablename] = table[self.get_columns(tablename, index=True)]
        utilityoutput.write_to_zip(tables, feedname)

    def get_table(self, tablename, index=True, original=False):
        if tablename not in self._tables.keys():
            return pd.DataFrame()
        
        table = self._tables[tablename]

        df = table.get_df(original=original)
        if not index and len(table.index):
            df = df.reset_index()

        return df.copy()

    def has_table(self, tablename, check_empty=True):
        if check_empty:
            return tablename in self._tables.keys() and not self.get_table(tablename).empty
        return tablename in self._tables.keys()

    def table_has_column(self, tablename, column):
        return column in self.get_columns(tablename)

    def get_columns(self, tablename, index=False):
        if (not self.has_table(tablename, check_empty=False)):
            return []
        return self._tables[tablename].get_columns(index=index)

    def update_table(self, tablename, df, cascade=True, exclude_tables=None):
        if exclude_tables is None: 
            exclude_tables = []
        if not self.has_table(tablename):
            return
        table = self._tables[tablename]
        table.update(df)

        if cascade:
            self.update_downstream_tables(tablename, exclude_tables)
            self.update_upstream_tables(tablename, exclude_tables)

    def update_downstream_tables(self, tablename, exclude_tables=None):
        # removes downstream references that no longer exist
        if exclude_tables is None: 
            exclude_tables = []

        table = self._tables[tablename]
        original_df = table.get_df().copy()

        for col in table.downstream_columns.keys():
            downstream_obj = table.downstream_columns[col]
            
            # if multiple downstream tables (i.e. calendar and calendar_dates for trips),
            # supply all tables to prune function
            for ref in downstream_obj['references']:
                columns = {}
                columns[tablename] = col

                if 'with_table_col' in downstream_obj.keys():
                    source = [tablename]
                    with_tab, with_col = downstream_obj['with_table_col']
                    source.append(with_tab)
                    columns[with_tab] = with_col
                else:
                    source = tablename

                down_table = ref.table
                down_col = ref.column
                columns[down_table] = down_col
                cascade_row = ref.cascade_row
                
                if down_table not in exclude_tables and self.has_table(down_table):
                    if cascade_row:
                        updated_table = self.prune_table_from_table_column(down_table, source, columns=columns)
                    else:
                        # TODO cleanup -- this call can only accept a single source, so source here must be a single table name
                        updated_table = self.remove_invalid_references_to_table_column(down_table, down_col, source, col)
                    if tablename not in exclude_tables:
                        exclude_tables.append(tablename)
                    if not updated_table.equals(original_df):
                        self.update_table(down_table, updated_table, exclude_tables=exclude_tables, cascade=cascade_row)

    def update_upstream_tables(self, tablename, exclude_tables=None):
        # removes upstream references that are no longer used
        if exclude_tables is None: 
            exclude_tables = []

        table = self._tables[tablename]
        original_df = table.get_df().copy()

        for col in table.upstream_columns.keys():
            upstream = table.upstream_columns[col]

            for ref in upstream['references']:
                up_table = ref.table
                up_col = ref.column
                cascade_row = ref.cascade_row
                if up_table not in exclude_tables and self.has_table(up_table):
                    columns = {}
                    columns[tablename] = col
                    columns[up_table] = up_col
                    if cascade_row:
                        updated_table = self.prune_table_from_table_column(up_table, tablename, columns=columns)
                    else:
                        updated_table = self.remove_invalid_references_to_table_column(up_table, up_col, tablename, col)
                    if tablename not in exclude_tables:
                        exclude_tables.append(tablename)
                    if not updated_table.equals(original_df):
                        self.update_table(up_table, updated_table, exclude_tables=exclude_tables, cascade=cascade_row)

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

    def defines_service_in_calendar_dates(self):
        # Returns true if the feed defines all service using calendar_dates
        if (not self.has_table('calendar')):
            return True

        calendars_dows = self.get_table('calendar')[DOWS]
        has_any_service = calendars_dows.any(axis=None)

        return has_any_service

    def run_function_on_all_tables(self, func, cascade=True):
        # func should be a function that accepts a df

        for tablename in self._tables.keys():
            df = self.get_table(tablename)
            new_df = func(df)
            self.update_table(tablename, new_df, cascade=cascade)

    def copy(self):
        c = GTFS()
        table_copies = {}
        for tablename in self._tables.keys():
            table_copies[tablename] = self._tables[tablename].copy()
        c._tables = table_copies
        return c

    def remove_invalid_references_to_table_column(self, target, target_col, source, source_col):
        # unsets values in target table column that do not exist in source table column
        # source can only be string here -- no practical need for multiple sources

        if not self.has_table(target): return pd.DataFrame()
        target_df = self.get_table(target, index=False)

        if not self.has_table(source): return target_df
        source_df = self.get_table(source, index=False)

        target_df['ref_valid'] = (target_df[target_col].isna() | target_df[target_col].isin(source_df[source_col]))

        target_df[target_col] = target_df.apply(lambda row: row[target_col] if row['ref_valid'] == True else np.nan, axis=1)

        return target_df

    def prune_table_from_table_column(self, target, source, columns={}):
        # remove rows from target if column value not present in same column of source,  e.g.:
        #   prune_table_from_table_column('transfers', 'stops', columns={'transfers': 'from_stop_id', 'stops': 'stop_id'})

        if not self.has_table(target): return pd.DataFrame()

        target_df = self.get_table(target, index=False)
        target_col = columns[target]

        if isinstance(source, list):
            # make source columns consistent before concat
            # new col name doesn't matter as long as its unique within table
            source_dfs = {}
            for tablename in source:
                if self.has_table(tablename, check_empty=False):
                    source_dfs[tablename] = self.get_table(tablename, index=False)

            source_col = 'source_col'
            for tablename in source_dfs.keys():
                col_rename = {}
                col_rename[columns[tablename]] = source_col
                source_dfs[tablename] = source_dfs[tablename].rename(columns=col_rename)
                columns[tablename] = source_col
            
            source_df = pd.concat( \
                list(source_dfs.values()),
                axis=0,
                ignore_index=True
            )
        else:
            if not self.has_table(source): return target_df

            source_df = self.get_table(source, index=False)
            source_col = columns[source]
        target_pruned = target_df[target_df[target_col].isna() | (target_df[target_col].isin(source_df[source_col]))]
        
        return target_pruned
