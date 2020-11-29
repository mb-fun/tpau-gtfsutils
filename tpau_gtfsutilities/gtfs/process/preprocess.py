import pandas as pd

def remove_all_wrapping_quotations_in_gtfs(gtfs):
    gtfs.run_function_on_all_tables(remove_wrapping_quotations_in_table)

def remove_wrapping_quotations_in_table(df):
    def strip_quotes_from_strings(x):
        if isinstance(x, str):
            # allow wrapping quotes if quotation marks or commas used within field
            # http://gtfs.org/reference/static/#file-requirements

            needs_quotes = x.count('"') > 2 or x.count(',') > 0
            if not needs_quotes:
                return x.strip('\"')
        return x

    return df.applymap(strip_quotes_from_strings)