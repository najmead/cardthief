from datetime import date, timedelta

today = date(2016, 4, 1)
wayBack = date(2014, 1, 1)

print(today)

d = [wayBack, today]

print(d)

date_set = set(d[0] + timedelta(x) for x in range((d[-1] - d[0]).days))

print(date_set)

missing = sorted(date_set - set(d))

print(missing)
[datetime.date(2010, 2, 27), datetime.date(2010, 2, 28)]