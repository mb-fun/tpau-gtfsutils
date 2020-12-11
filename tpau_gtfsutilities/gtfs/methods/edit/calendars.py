from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs

def remove_exception_calendars():
    if gtfs.defines_service_in_calendar_dates():
        return
    
    gtfs.clear_table('calendar_dates')