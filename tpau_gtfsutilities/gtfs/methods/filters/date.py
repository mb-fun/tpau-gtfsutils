from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs
from tpau_gtfsutilities.gtfs.methods.helpers.triphelpers import get_trips_extended
from tpau_gtfsutilities.helpers.datetimehelpers import GTFSDate

def filter_trips_by_date(date):
    # removes trips that do not occur on specified date
    # TODO consider replacing with filter_calendars_by_date, prune

    trips_extended = get_trips_extended()

    dow = GTFSDate(date).dow()
    date = int(date)


    if gtfs.has_table('calendar'):
        date_in_range = (trips_extended['start_date'] <= date) & (date <= trips_extended['end_date'])
        dow_in_service = trips_extended[dow] == 1

        trips_filter = date_in_range & dow_in_service
    
    # filter calendar_dates for relevant calendar exceptions
    if gtfs.has_table('calendar_dates'):
        calendar_dates = gtfs.get_table('calendar_dates')
        added_on_date = (calendar_dates['date'] == date) & (calendar_dates['exception_type'] == 1)
        services_added_on_date = calendar_dates[added_on_date]['service_id']

        removed_on_date = (calendar_dates['date'] == date) & (calendar_dates['exception_type'] == 2)
        services_removed_on_date = calendar_dates[removed_on_date]['service_id']
        service_added_on_date = trips_extended['service_id'].isin(services_added_on_date)
        service_removed_on_date = trips_extended['service_id'].isin(services_removed_on_date)

        if gtfs.has_table('calendar'):
            trips_filter = (date_in_range & dow_in_service & ~service_removed_on_date) | service_added_on_date
        else:
            trips_filter = service_added_on_date

    trips_filtered_df = trips_extended[trips_filter]

    gtfs.update_table('trips', trips_filtered_df)

