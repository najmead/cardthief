import sqlite3

from datetime import date, timedelta

conn = sqlite3.connect('netrunner.db')

conn.row_factory = sqlite3.Row

c = conn.cursor()

c.execute('''
            select distinct date(create_date) 
            from deck 
            order by date(create_date);
        ''')

dates = c.fetchall()

today = datetime.today()-timedelta(days=1)
today = today.replace(hour=0, minute=0, second=0, microsecond=0)
wayBack = wayBack = datetime.strptime('2016-01-27', "%Y-%m-%d")

d = [today, wayBack]

existing = set(datetime.strptime(dates[x][0],"%Y-%m-%d") for x in range(0, len(dates)))

d.extend(existing)

d = sorted(d)

allDates = set(d[0] + timedelta(x) for x in range((d[-1] - d[0]).days))

missingDates = sorted(allDates - set(d))

print(missingDates)
