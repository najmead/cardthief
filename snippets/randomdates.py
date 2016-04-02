import random
import datetime

random.seed(123)

days = random.sample(range(0, 1000), 1000)

currentDate = datetime.datetime.today()

for day in days:
    queryDate = (currentDate+datetime.timedelta(days=day)).strftime('%Y-%m-%d')
    print(queryDate)
    
currentDate+datetime.timedelta(days=scanDays)).strftime('%Y-%m-%d %H:%M:%S')
print(numbers)