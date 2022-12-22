import pandas as pd
import datetime
import os

def get_weather():
    import io
    import sys
    import urllib.request
    
    
    start_date = str(datetime.datetime.now()+ datetime.timedelta(days=1))
    end_date = str(datetime.datetime.now() + datetime.timedelta(days=8))
    visual_crossing_key = os.environ["VISUAL_CROSSING_KEY"]

    try:
        ResultBytes = (
            urllib.request.urlopen(
                f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Stockholm/{start_date}/{end_date}?unitGroup=metric&elements=name%2Clatitude%2Clongitude%2Ctemp%2Cwindgust%2Cwindspeed%2Cwinddir%2Ccloudcover%2Csource&include=days%2Cobs&key={visual_crossing_key}&contentType=csv"
            )
            .read()
            .decode("UTF-8")
        )

        weather_data = pd.read_csv(io.StringIO(ResultBytes))
        weather_data["date"] = pd.date_range(
            start=start_date, periods=len(weather_data), freq="D"
        )

    except urllib.error.HTTPError as e:
        ErrorInfo = e.read().decode()
        print("Error code: ", e.code, ErrorInfo)
        sys.exit()
    except urllib.error.URLError as e:
        ErrorInfo = e.read().decode()
        print("Error code: ", e.code, ErrorInfo)
        sys.exit()

    weather_data = weather_data.dropna(axis=0)
    weather_data = weather_data[
        ["temp", "windgust", "windspeed", "winddir", "cloudcover", "date"]
    ].copy()
    weather_data["date"] = pd.to_datetime(weather_data["date"])
    weather_data = weather_data.set_index("date")
    weather_data = weather_data.reset_index()
    weather_data["date"] = weather_data["date"].dt.strftime("%Y-%m-%d")
    return weather_data

def get_date_data():
    start_date = str(datetime.datetime.now()+ datetime.timedelta(days=1))
    end_date = str(datetime.datetime.now()+ datetime.timedelta(days=8))
    date_list = pd.date_range(start=start_date, end=end_date)
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
    return dates_data

def get_energy_data():
    from entsoe import EntsoePandasClient

    client = EntsoePandasClient(api_key=os.environ["ENTSOE_KEY"])

    start_date = pd.Timestamp(datetime.datetime.now(), tz="Europe/Berlin")
    end_date = pd.Timestamp(
        datetime.datetime.now() + datetime.timedelta(days=1), tz="Europe/Berlin"
    )
    country_code = "SE_3"

    day_ahead_prices = client.query_day_ahead_prices(
        country_code, start=start_date, end=end_date
    )
    load = client.query_load(country_code, start=start_date, end=end_date)
    aggregate_water_reservoirs_and_hydro_storage = (
        client.query_aggregate_water_reservoirs_and_hydro_storage(
            country_code, start=start_date, end=end_date
        )
    )

    result = pd.concat(
        [day_ahead_prices, load, aggregate_water_reservoirs_and_hydro_storage], axis=1
    )
    result = result.ffill()

    energy_data = pd.concat([energy_data, result], axis=0)

    energy_data = energy_data.dropna(axis=0)
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