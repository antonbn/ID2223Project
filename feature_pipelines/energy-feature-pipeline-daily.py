import os
import sys

import modal

LOCAL = False
#LOCAL = True
#from keys import entsoe_key

if LOCAL == False:
    stub = modal.Stub("energy-feature-pipeline-daily")
    image = modal.Image.debian_slim().pip_install(["hopsworks", "entsoe-py", "pandas"])

    @stub.function(
        image=image,
        schedule=modal.Cron("00 08 * * *"),
        secret=modal.Secret.from_name("id2223-project"),
    )
    def f():
        g()


def g():
    import datetime

    import hopsworks
    import pandas as pd

    # pandas_image = modal.Image.conda().conda_install("pandas==1.4.0")
    from entsoe import EntsoePandasClient

    client = EntsoePandasClient(api_key=os.environ["ENTSOE_KEY"])
    #client = EntsoePandasClient(api_key=entsoe_key)

    start_date = pd.Timestamp(
        datetime.datetime.now() + datetime.timedelta(days=-7), tz="Europe/Berlin"
    )
    end_date = pd.Timestamp(
        datetime.datetime.now() + datetime.timedelta(days=1), tz="Europe/Berlin"
    )
    country_code = "SE_3"

    day_ahead_prices = client.query_day_ahead_prices(
        country_code, start=start_date, end=end_date
    )
    load = client.query_load_forecast(country_code, start=start_date, end=end_date)
    i = 0
    while True:
        if i < -40:
            sys.exit()
        try:
            aggregate_water_reservoirs_and_hydro_storage = (
                client.query_aggregate_water_reservoirs_and_hydro_storage(
                    country_code,
                    start=pd.Timestamp(
                        datetime.datetime.now() + datetime.timedelta(days=i),
                        tz="Europe/Berlin",
                    ),
                    end=pd.Timestamp(
                        datetime.datetime.now() + datetime.timedelta(days=8 + i),
                        tz="Europe/Berlin",
                    ),
                )
            )

            break
        except:
            i -= 1
    energy_data = pd.concat(
        [day_ahead_prices, load, aggregate_water_reservoirs_and_hydro_storage], axis=1
    )
    energy_data = energy_data.ffill()
    if energy_data.isnull().values.any():
        # print(f'startdate: {start_date}, enddate: {end_date}')
        print(energy_data)
    energy_data = energy_data.dropna(axis=0)
    energy_data = energy_data.reset_index()
    energy_data.columns = ["date", "price", "load", "filling_rate"]
    energy_data["date"] = pd.to_datetime(energy_data["date"])
    energy_data = energy_data.set_index("date")
    energy_data = energy_data.resample("D").mean()
    energy_data = energy_data.reset_index()
    energy_data["date"] = energy_data["date"].dt.strftime("%Y-%m-%d")
    energy_data["p_1"] = energy_data["price"].shift()
    energy_data["p_2"] = energy_data["price"].shift(2)
    energy_data["p_3"] = energy_data["price"].shift(3)
    energy_data["p_4"] = energy_data["price"].shift(4)
    energy_data["p_5"] = energy_data["price"].shift(5)
    energy_data["p_6"] = energy_data["price"].shift(6)
    energy_data["p_7"] = energy_data["price"].shift(7)
    energy_data = energy_data.dropna()
    energy_data = energy_data.reset_index(drop=True)
    energy_data = energy_data.drop(index=0)
    energy_data = energy_data.reset_index(drop=True)
    # print('after: ')
    # print(energy_data.dtypes)

    project = hopsworks.login()
    fs = project.get_feature_store()

    energy_fg = fs.get_or_create_feature_group(
        name="energy_prices",
        version=1,
        primary_key=[
            "date",
            "load",
            "filling_rate",
            "p_1",
            "p_2",
            "p_3",
            "p_4",
            "p_5",
            "p_6",
            "p_7",
        ],
        description="daily energy prices",
    )

    # print('energy feature group')
    # print(energy_fg.read().iloc[[-1]])
    # print(type(energy_fg.read().iloc[[-1]]))
    # energy_fg.commit_delete_record(energy_fg.read().iloc[[-1]])
    # print(energy_fg.read())
    
    try:
        energy_fg.insert(energy_data, write_options={"wait_for_job": False})
    except:
        energy_data["date"] = energy_data["date"].astype("string")
        energy_fg.insert(energy_data, write_options={"wait_for_job": False})


if __name__ == "__main__":
    if LOCAL == True:
        g()
    else:
        with stub.run():
            f()
