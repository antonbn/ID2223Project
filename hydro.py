import datetime
import os
import sys

import pandas as pd
from entsoe import EntsoePandasClient

from keys import entsoe_key

client = EntsoePandasClient(api_key=entsoe_key)

start_date = pd.Timestamp(
    datetime.datetime.now() + datetime.timedelta(days=1), tz="Europe/Berlin"
)
end_date = pd.Timestamp(
    datetime.datetime.now() + datetime.timedelta(days=8), tz="Europe/Berlin"
)
country_code = "SE_3"

# day_ahead_prices = client.query_day_ahead_prices(
#     country_code, start=start_date, end=end_date
# )
load = client.query_load_forecast(country_code, start=start_date, end=end_date)
i = 0
while True:
    if i < -10:
        sys.exit()
    try:
        aggregate_water_reservoirs_and_hydro_storage = (
            client.query_aggregate_water_reservoirs_and_hydro_storage(
                country_code,
                start=pd.Timestamp(
                    datetime.datetime.now() - datetime.timedelta(days=i),
                    tz="Europe/Berlin",
                ),
                end=pd.Timestamp(
                    datetime.datetime.now() + datetime.timedelta(days=8 - i),
                    tz="Europe/Berlin",
                ),
            )
        )

        break
    except:
        i -= 1

energy_data = pd.concat([load, aggregate_water_reservoirs_and_hydro_storage], axis=1)
energy_data = energy_data.ffill()


energy_data = energy_data.dropna(axis=0)
energy_data.columns = ["date", "load", "filling_rate"]
energy_data["date"] = pd.to_datetime(energy_data["date"])
energy_data = energy_data.set_index("date")
energy_data = energy_data.resample("D").mean()
energy_data = energy_data.reset_index()
energy_data["date"] = energy_data["date"].dt.strftime("%Y-%m-%d")
# energy_data["p_1"] = energy_data["price"].shift()
# energy_data["p_2"] = energy_data["price"].shift(2)
# energy_data["p_3"] = energy_data["price"].shift(3)
# energy_data["p_4"] = energy_data["price"].shift(4)
# energy_data["p_5"] = energy_data["price"].shift(5)
# energy_data["p_6"] = energy_data["price"].shift(6)
# energy_data["p_7"] = energy_data["price"].shift(7)
energy_data = energy_data.dropna()
energy_data = energy_data.reset_index(drop=True)
energy_data = energy_data.drop(index=0)
energy_data = energy_data.reset_index(drop=True)
print(energy_data)
