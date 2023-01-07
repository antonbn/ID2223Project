import datetime
import os
import sys

import hopsworks
import joblib
import modal
import pandas as pd

LOCAL = False

if LOCAL == False:
    stub = modal.Stub("inference-pipeline")
    image = modal.Image.debian_slim().pip_install(
        [
            "hopsworks",
            "joblib==1.2.0",
            "xgboost==1.6.2",
            "pandas==1.5.2",
            "entsoe-py",
            "scikit-learn==1.2.0",
            "pandera[io]",
        ]
    )

    @stub.function(
        image=image,
        schedule=modal.Cron("01 05 * * *"),
        secret=modal.Secret.from_name("id2223-project"),
        mounts=[
            *modal.create_package_mounts(["utils"]),
            modal.Mount(
                local_dir=r"C:/Users/Isac/Documents/CDATE5 ML2/ID2223/project/utils/utils/",
                remote_dir="/",
            ),
            modal.Mount(
                local_dir=r"C:/Users/Isac/Documents/CDATE5 ML2/ID2223/project/pandera_schemas/",
                remote_dir="/panderas_schemas",
            ),
        ],
    )
    def f():
        g()


def get_weather_data():
    import io
    import sys
    import urllib.request

    start_date = str(
        (datetime.datetime.now() + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    )
    end_date = str(
        (datetime.datetime.now() + datetime.timedelta(days=8)).strftime("%Y-%m-%d")
    )
    visual_crossing_key = os.environ["VISUAL_CROSSING_KEY"]

    try:
        ResultBytes = (
            urllib.request.urlopen(
                f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Stockholm/{start_date}/{end_date}?unitGroup=metric&elements=name%2Clatitude%2Clongitude%2Ctemp%2Cwindgust%2Cwindspeed%2Cwinddir%2Ccloudcover%2Csource&include=days%2Cfcst&key={visual_crossing_key}&contentType=csv"
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


def get_dates():
    from utils import get_dates_data

    return get_dates_data(
        start_date=datetime.datetime.now() + datetime.timedelta(days=1),
        end_date=datetime.datetime.now() + datetime.timedelta(days=8),
    )


def get_energy_data():
    from entsoe import EntsoePandasClient

    client = EntsoePandasClient(api_key=os.environ["ENTSOE_KEY"])
    # client = EntsoePandasClient(api_key=entsoe_key)

    start_date = pd.Timestamp(
        datetime.datetime.now() + datetime.timedelta(days=1), tz="Europe/Berlin"
    )
    end_date = pd.Timestamp(
        datetime.datetime.now() + datetime.timedelta(days=8), tz="Europe/Berlin"
    )

    country_code = "SE_3"

    load_forecast = client.query_load_forecast(
        country_code, start=start_date, end=end_date, process_type="A31"
    )
    load_forecast = load_forecast.reset_index()
    load_forecast["load"] = (
        load_forecast["Max Forecasted Load"] + load_forecast["Min Forecasted Load"]
    ) / 2

    last_day_values = query_last_day().reset_index()

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
    energy_data = pd.DataFrame({"date": pd.date_range(start_date, end_date)})
    energy_data["load"] = load_forecast["load"]
    energy_data["filling_rate"] = aggregate_water_reservoirs_and_hydro_storage[0]
    energy_data = energy_data.ffill()
    energy_data["p_1"] = last_day_values["price"]
    energy_data["p_2"] = last_day_values["p_1"]
    energy_data["p_3"] = last_day_values["p_2"]
    energy_data["p_4"] = last_day_values["p_3"]
    energy_data["p_5"] = last_day_values["p_4"]
    energy_data["p_6"] = last_day_values["p_5"]
    energy_data["p_7"] = last_day_values["p_6"]
    energy_data["date"] = energy_data["date"].dt.strftime("%Y-%m-%d")

    energy_data.iloc[1, 3:] = energy_data.iloc[0, 2:-1]
    energy_data.iloc[2, 4:] = energy_data.iloc[1, 3:-1]
    energy_data.iloc[3, 5:] = energy_data.iloc[2, 4:-1]
    energy_data.iloc[4, 6:] = energy_data.iloc[3, 5:-1]
    energy_data.iloc[5, 7:] = energy_data.iloc[4, 6:-1]
    energy_data.iloc[6, 8:] = energy_data.iloc[5, 7:-1]
    energy_data.iloc[7, 9:] = energy_data.iloc[6, 8:-1]
   
    return energy_data


def query_last_day():
    project = hopsworks.login()
    fs = project.get_feature_store()

    energy_fg = fs.get_feature_group(
        name="energy_prices",
        version=1,
    ).read()

    return energy_fg.loc[energy_fg["date"] == energy_fg["date"].max()]


def g():
    from pandera import DataFrameSchema

    project = hopsworks.login()
    mr = project.get_model_registry()
    model = mr.get_model("price_modal", version=1)
    model_dir = model.download()
    model = joblib.load(model_dir + "/price_model.pkl")

    weather_data, energy_data, dates_data = (
        get_weather_data(),
        get_energy_data(),
        get_dates(),
    )
    weather_schema = DataFrameSchema.from_json(
        r"/panderas_schemas/weather-feature-pipeline-daily-schema.json"
    )
    energy_schema = DataFrameSchema.from_json(
        r"/panderas_schemas/energy-feature-pipeline-daily-schema.json"
    ).remove_columns(["price", "p_1", "p_2", "p_3", "p_4", "p_5", "p_6", "p_7"])
    dates_schema = DataFrameSchema.from_json(
        r"/panderas_schemas/date-feature-pipeline-daily-schema.json"
    )

    weather_data = weather_schema.validate(weather_data)
    energy_data = energy_schema.validate(energy_data)
    dates_data = dates_schema.validate(dates_data)

    wd = weather_data.merge(dates_data, on="date")
    features = wd.merge(energy_data, on="date")

    price_pred_list = []
    for i in range(0, 7):
        price_pred= model.predict(features.set_index("date").iloc[[i]])[0]
        price_pred_list.append(price_pred)

        for j in range(i + 1, 8):
            features.loc[j, f"p_{j - i}"] = price_pred

    features = features.reset_index()
    features = features.drop(index=7)

    price_predictions = pd.DataFrame()
    price_predictions["date"] = features["date"]
    price_predictions["days_ahead"] = [i for i in range(1, 8)]
    price_predictions["predicted_price"] = price_pred_list
    print(price_predictions)
    print(price_predictions["predicted_price"])
    fs = project.get_feature_store()

    price_pred_fg = fs.get_or_create_feature_group(
        name="price_predictions",
        version=1,
        primary_key=["date", "days_ahead"],
        description="predicted prices",
    )
    price_pred_fg.insert(price_predictions, write_options={"wait_for_job": False})


if __name__ == "__main__":
    if LOCAL == True:
        g()
    else:
        with stub.run():
            f()
