import pandas as pd
from tpau_gtfsutilities.gtfs.properties import NUMERIC_DTYPES

class GTFSTable:
    index = []
    df = None
    original_df = None
    columns = []
    # dict of columns -> downstream references in table that other tables
    # refer to -- used for cascading data removal
    # e.g. for calendar: \
    #   { 'service_id': {
    #       references: [ColumnRef('trips', 'service_id')],
    #       with_table: ('calendar_dates', 'service_id')
    #   }}
    downstream_columns = {}
    # dict of columns -> upstream references in table for columns that determine an entities
    # existence/usage (used for pruning).
    # e.g. an entry for trips would look like this, since a calendar can be removed if not used on any trips:
    #   { 'service_id': {
    #        references: [ColumnRef('calendar', 'service_id'), ColumnRef('calendar_dates', 'service_id')]
    #    }}
    # But 'route_id' for example would NOT be an upstream reference from fare_rules to routes.
    # when multiple tables supplied like above, column can reference either table as source id, so 
    # for example trips can reference calendar or calendar_dates for regular or exception-only calendars
    upstream_columns = {}
    
    def __init__(self, csv=None, df=None, dtype={}):
        # must provide either csv or df

        if csv:
            df = pd.read_csv(csv, dtype=dtype)

        self.dtype = dtype
        self.columns = df.columns.tolist()
        self.original_df = df.copy()
        self.df = self.clean(df)

    def update(self, df):
        # updates dataframe (disallowing column changes) and trigger downstream and 
        # upstream changes

        self.df = self.clean(df)

    def get_df(self, original=False):
        if original:
            return self.clean(self.original_df).copy()
        return self.df.copy()

    def clean(self, df):
        # returns df with index correctly set (regardless of df), dtype set
        # and columns selected

        def clean_empty_vals(dataframe):
            # make sure empty values are set to empty strings if string column
            for col in self.dtype.keys():
                dt = self.dtype[col]
                if dt == 'str':
                    dataframe[col].fillna('', inplace=True)
            return dataframe

        if len(self.index):
            df = df.reset_index()
            df = clean_empty_vals(df)
            df = df.astype(self.dtype)
            df = df.set_index(self.index)
        else:
            df = clean_empty_vals(df)
            df = df.astype(self.dtype)

        return df[self.get_columns()]

    def get_columns(self, index=False):
        if not index:
            columns = list(filter(lambda x: x not in self.index, self.columns.copy()))
        else:
            columns = self.columns.copy()

        return columns

    def copy(self):
        # returns a new class instance with
        # df and original_df set to current values
        table_copy = self.__class__(df=self.original_df, dtype=self.dtype)
        table_copy.update(self.df.copy())
        return table_copy
