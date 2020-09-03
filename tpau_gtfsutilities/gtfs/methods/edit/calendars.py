from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs

def remove_exception_calendars():
    if gtfs.only_uses_calendar_dates():
        return
    
    gtfs.clear_table('calendar_dates')