import numpy as np
from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.helpers.datetimehelpers import GTFSDateRange

def filter_calendars_by_daterange(daterange):
    calendar = gtfs.get_table('calendar')
    filter_daterange = GTFSDateRange(daterange['start'], daterange['end'])

    calendar['_daterange'] = calendar.apply(lambda row: GTFSDateRange(row['start_date'], row['end_date']), axis=1)

    calendar['_overlap'] = calendar['_daterange'].apply(lambda dr: \
        filter_daterange.overlap(dr) \
    )

    cols = gtfs.get_columns('calendar', index=False)

    calendar_filtered = calendar[calendar['_overlap'].notnull()]

    gtfs.update_table('calendar', calendar_filtered[cols])
    

