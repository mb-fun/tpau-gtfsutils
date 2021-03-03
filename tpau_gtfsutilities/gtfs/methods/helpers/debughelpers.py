from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs

def print_table(tablename, message='', all=False):
    df = gtfs.get_table(tablename)
    prefix = 'DEBUG ' + tablename + ' ' + message + ': '
    if all:
        print(prefix, df.to_string())
    else:
        print(prefix, df)