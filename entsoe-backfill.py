import time
from os.path import exists

import hopsworks
import pandas as pd
from entsoe import EntsoePandasClient

from keys import entsoe_key

if not exists("data/entsoe_backfill.csv"):

    client = EntsoePandasClient(api_key=entsoe_key)

    start_date = pd.Timestamp("20221219", tz="Europe/Berlin")
    end_date = pd.Timestamp("20221230", tz="Europe/Berlin")
    country_code = "SE_3"

    energy_data = client.query_day_ahead_prices(
        country_code, start=start_date, end=end_date
    )
        

    energy_data.to_csv("data/entsoe_backfill.csv")

else:
    energy_data = pd.read_csv("data/entsoe_backfill.csv")

energy_data = energy_data.dropna(axis=0)
energy_data["date"] = pd.to_datetime(energy_data["date"])
energy_data = energy_data.set_index("date")
energy_data = energy_data.resample("D").mean()
energy_data = energy_data.reset_index()
energy_data["date"] = energy_data["date"].dt.strftime("%Y-%m-%d")
energy_data = energy_data.dropna()
energy_data = energy_data.reset_index(drop=True)
energy_data.to_csv("data/entsoe_backfill_daily.csv")
# energy_data = energy_data.dropna(axis=0)
# energy_data.columns = ["date", "price", "load", "filling_rate"]
# energy_data["date"] = pd.to_datetime(energy_data["date"])
# energy_data = energy_data.set_index("date")
# energy_data = energy_data.resample("D").mean()
# energy_data = energy_data.reset_index()
# energy_data["date"] = energy_data["date"].dt.strftime("%Y-%m-%d")
# energy_data = energy_data.dropna()
# energy_data = energy_data.reset_index(drop=True)



