import datetime

def dow_from_date(date):
    # input: Numerical date in YYYYMMDD format
    # returns: day of week as full lowercase string (i.e., "sunday")

    datestring = str(date)
    year = datestring[:4]
    month = datestring[4:6]
    day = datestring[-2:]

    weekday = datetime.date(int(year), int(month), int(day)).strftime('%A')
    return weekday.lower()

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