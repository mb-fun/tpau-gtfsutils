from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.gtfs.methods.helpers.triphelpers import get_trips_extended
from tpau_gtfsutilities.helpers.datetimehelpers import GTFSDate

def filter_trips_by_date(date):
    # removes trips that do not occur on specified date
    # TODO consider replacing with filter_calendars_by_date, prune

    trips_extended = get_trips_extended()

    # filter calendars on date range
    dow = GTFSDate(date).dow()
    date = int(date)

    # TODO handle missing calendar_dates
    # filter calendar_dates for relevant calendar exceptions
    calendar_dates = gtfs.get_table('calendar_dates')

    added_on_date = (calendar_dates['date'] == date) & (calendar_dates['exception_type'] == 1)
    services_added_on_date = calendar_dates[added_on_date]['service_id']

    removed_on_date = (calendar_dates['date'] == date) & (calendar_dates['exception_type'] == 2)
    services_removed_on_date = calendar_dates[removed_on_date]['service_id']

    # filter trips and write to table
    date_in_range = (trips_extended['start_date'] <= date) & (date <= trips_extended['end_date'])
    dow_in_service = trips_extended[dow] == 1
    service_added_on_date = trips_extended['service_id'].isin(services_added_on_date)
    service_removed_on_date = trips_extended['service_id'].isin(services_removed_on_date)

    trips_filter = (date_in_range & dow_in_service & ~service_removed_on_date) | service_added_on_date
    trips_filtered_df = trips_extended[trips_filter]

    gtfs.update_table('trips', trips_filtered_df, allow_column_changes=False)

