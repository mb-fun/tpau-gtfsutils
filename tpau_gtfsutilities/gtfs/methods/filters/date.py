import datetime

from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.gtfs.methods.helpers.triphelpers import get_trips_extended
from tpau_gtfsutilities.helpers.datetimehelpers import dow_from_date

# def filter_trips_by_date(trips_extended, trips, calendar_dates):
def filter_trips_by_date(date):
    # removes trips that do not occur on specified date

    trips_extended = get_trips_extended()

    # filter calendars on date range
    dow = dow_from_date(date)
    date = int(date)

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

    trips_columns = gtfs.get_columns('trips', index=False)

    gtfs.update_table('trips', trips_filtered_df[trips_columns])

