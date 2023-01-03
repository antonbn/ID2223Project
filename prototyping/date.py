import datetime

import hopsworks
import pandas as pd

start_date = str(datetime.datetime.now())
end_date = str(datetime.datetime.now() + datetime.timedelta(days=1))
date_list = pd.date_range(start=start_date, end=start_date)
dates_data = pd.DataFrame(data={"date": date_list})
dates_data["dayofyear"] = dates_data["date"].dt.dayofyear
dates_data["dayofweek"] = dates_data["date"].dt.dayofweek
dates_data["month"] = dates_data["date"].dt.month
dates_data["week"] = dates_data["date"].dt.isocalendar().week

dates_data = dates_data.dropna(axis=0)
dates_data["date"] = pd.to_datetime(dates_data["date"])
dates_data = dates_data.set_index("date")
dates_data = dates_data.reset_index()

dates_data["date"] = dates_data["date"].dt.strftime("%Y-%m-%d")
print(dates_data)