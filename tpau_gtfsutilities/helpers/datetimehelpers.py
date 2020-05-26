import datetime
import numpy as np

class GTFSDateRange:
    start = None
    end = None

    def __init__(self, start_date, end_date):
        self.start = start_date if isinstance(start_date, GTFSDate) else GTFSDate(start_date)
        self.end = end_date if isinstance(end_date, GTFSDate) else GTFSDate(end_date)

    def num_days(self):
        return self.end.diff_days(self.start)

    def days_of_week(self):
        # returns list of full lowercase day-of-week strings
        current_date = self.start.date
        dows = []
        while current_date <= self.end.date and len(dows) < 7:
            dow = current_date.strftime('%A').lower()
            dows.append(dow)
            current_date = current_date + datetime.timedelta(days=1)
        return dows

    def includes_range(self, other_daterange, partial=True):
        # partial indicates whether to return true if only part of the other daterange is included
        starts_after = self.start.before(other_daterange.start, inclusive=True)
        ends_before = self.end.after(other_daterange.end, inclusive=True)
        if partial:
            starts_after or ends_before
        return starts_after and ends_before

    def overlap(self, other_daterange):
        max_start = max(self.start.date, other_daterange.start.date)
        min_end = min(self.end.date, other_daterange.end.date)

        # invalid, no overlap
        if max_start > min_end:
            return None

        return GTFSDateRange(max_start, min_end)



class GTFSDate:
    date = None

    def __init__(self, date):
        # TODO validate YYYYMMDD format
        self.date = date if isinstance(date, datetime.date) else to_date(date)

    def dow(self):
        # returns full lowercase day of week string, i.e. 'sunday'
        return self.date.strftime('%A').lower()

    def diff_days(self, date):
        other = date.date() if isinstance(date, GTFSDate) else GTFSDate(date).date()
        delta = self.date - other
        return delta.days

    def before(self, date, inclusive=False):
        other = date if isinstance(date, GTFSDate) else GTFSDate(date)
        if inclusive:
            return self.date <= other.date()
        return self.date < other.date()
    
    def after(self, date, inclusive=False):
        other = date if isinstance(date, GTFSDate) else GTFSDate(date)
        if inclusive:
            return self.date >= other.date()
        return self.date > other.date()

def to_date(gtfs_datestring):
    gtfs_datestring = str(gtfs_datestring)
    year = gtfs_datestring[:4]
    month = gtfs_datestring[4:6]
    day = gtfs_datestring[-2:]

    return datetime.date(int(year), int(month), int(day))

def to_gtfs_datestring(date):
    return date.strftime('%Y%m%d')

def seconds_to_military(seconds_since_zero):
    # returns military time string from "seconds since zero"

    hours, seconds_left = divmod(seconds_since_zero, 3600)
    minutes, seconds = divmod(seconds_left, 60)
    return datetime.time(hours, minutes, seconds).strftime('%H:%M:%S')


def seconds_since_zero(military):
    t = military.split(':')
    hours = int(t[0])
    minutes = int(t[1])
    seconds = int(t[2])

    return hours * 3600 + minutes * 60 + seconds