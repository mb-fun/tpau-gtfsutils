# Script for checking stop visit report against a TNeXT stops report
# Will print any non-matching visit counts

import argparse
import pandas as pd

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--utils-report', help="GTFS Utils stops visits report", required=True, nargs='?')
    parser.add_argument('-t', '--tnext-report', help="TNeXT stops visits report", required=True, nargs='?')

    args = parser.parse_args()
    utils_report = args.utils_report
    tnext_report = args.tnext_report

    util_cols = {
        'agency_id': 'agency_id',
        'stop_id': 'stop_id',
        'visits': 'visit_counts'
    }
    
    tnext_cols = {
        'agency_id': 'Agency ID',
        'stop_id': 'Stop ID',
        'visits': 'Visits (2)(3)(4)'
    }

    utils_df = pd.read_csv(utils_report)[util_cols.values()]
    tnext_df = pd.read_csv(tnext_report)[tnext_cols.values()]

    results = utils_df.merge( \
        tnext_df,
        how='left',
        left_on=[util_cols['agency_id'], util_cols['stop_id']],
        right_on=[tnext_cols['agency_id'], tnext_cols['stop_id']]
    )

    non_matching_results = results[results[util_cols['visits']] != results[tnext_cols['visits']]]
    non_matching_results = non_matching_results.rename(columns={ util_cols['visits']: 'GTFS Utils Visits', tnext_cols['visits']: 'TNeXT Visits' })
    if non_matching_results.empty:
        print('All visit counts match!')
    else:
        print('Stops with different visit counts: \n', non_matching_results.to_string())


if __name__ == '__main__':
    main()
