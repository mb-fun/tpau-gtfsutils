import pandas as pd
import datetime
import numpy as np

from tpau_gtfsutilities.gtfs.gtfssingleton import gtfs as gtfssingleton
from tpau_gtfsutilities.helpers.datetimehelpers import GTFSDateRange, GTFSDateRange

class GTFSServiceCalendar:
    daterange = None
    dows = {}
    added_dates = [] # list of datestrings
    removed_dates = [] # list of datestrings
    _gtfs = None

    def __init__(self, service_id, gtfs_override=None):
        # TODO: ensure this works for alternate calendars method

        self._gtfs = gtfs_override if gtfs_override else gtfs_singleton

        calendar_row = self._gtfs.get_table('calendar').loc[service_id]
        self.daterange = GTFSDateRange(calendar_row.loc['start_date'], calendar_row.loc['end_date'])

        # dows
        dow_list = ['monday', 'tuesday', 'wednesday', 'thursday', \
            'friday', 'saturday', 'sunday']
        for day in dow_list:
            self.dows[day] = (calendar_row.loc[day] == 1)

        # exceptions
        calendar_dates = self._gtfs.get_table('calendar_dates')
        exceptions = calendar_dates[calendar_dates['service_id'] == service_id]
        added_exceptions = exceptions[exceptions['exception_type'] == 1]
        self.added_dates = added_exceptions['date'].astype(str).tolist()
        removed_exceptions = exceptions[exceptions['exception_type'] == 2]
        self.removed_dates = removed_exceptions['date'].astype(str).tolist()

    def num_active_days(self):
        # hopefully this isn't slow on large calendars
        day_count = 0

        current_date = self.daterange.start

        # add service days, not including days with removed service
        while not current_date.after(self.daterange.end):
            if self.dows[current_date.dow()] and (current_date.datestring() not in self.removed_dates):
                day_count += 1
            current_date.add_days(1)

        # if added_dates are not in daterange or are not on served dow, add to day_count
        for ad in self.added_dates:
            added_date = GTFSDate(ad)
            if (not self.daterange.includes(added_date)) and (not self.dows[added_date.dow()]):
                day_count += 1

        return day_count
        