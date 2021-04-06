import pandas as pd

class GTFSCollection:
    # Used when simultaneous editing/analysis on GTFS feeds is needed (i.e. cluster stops)
    
    feeds = {} # dict of GTFS instances

    def __init__(self):
        return

    def add_feed(self, gtfs, name):
        # gtfs is an instance of GTFS class
        self.feeds[name] = gtfs

    def write_all_feeds(self):
        for name in self.feeds.keys():
            self.feeds[name].write_feed(name)

    def get_combined_gtfs_table(self, tablename):
        # returns combined df with no index
        
        table_dict = {}
        for feed in self.feeds.keys():
            gtfs = self.feeds[feed]
            if gtfs.has_table(tablename):
                table = gtfs.get_table(tablename, index=False)
                table_dict[feed] = table

        return combine_dfs_from_dict(table_dict, 'feed')

    def get_combined_computed_table(self, function):
        # function take GTFS instance as arg and returns dataframe
        # applies function to each GTFS and returns result as combined table

        df_dict = {}
        for feed in self.feeds.keys():
            gtfs = self.feeds[feed]
            df = function(gtfs).reset_index()
            # drop index column if created by reset_index
            if 'index' in df.columns.tolist():
                df = df.drop(columns=['index'])
            df_dict[feed] = df

        return combine_dfs_from_dict(df_dict, 'feed')

    def has_multiagency_feed(self):
        for feed in self.feeds.keys():
            gtfs = self.feeds[feed]
            if (gtfs.is_multiagency()):
                return True
        return False

def combine_dfs_from_dict(df_dict, key_name='key'):
    # returns concatenated dfs with dict key in new column
    # note that dicts can have different columns
    # returned df does not preserve index

    column_set = set([key_name])
    for key in df_dict.keys():
        df = df_dict[key]
        column_set = column_set.union(set(df.columns.tolist()))

    combined_df = pd.DataFrame()
    for key in df_dict.keys():
        df = df_dict[key]
        df[key_name] = key
        combined_df = pd.concat( \
            [combined_df, df.reset_index()],
            axis=0,
            ignore_index=True
        )

    return combined_df[column_set]