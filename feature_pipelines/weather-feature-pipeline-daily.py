import os

import modal

# from keys import visual_crossing_key

LOCAL = False
# LOCAL = True

if LOCAL == False:
    stub = modal.Stub("weather-feature-pipeline-daily")
    image = modal.Image.debian_slim().pip_install(["hopsworks", "pandera[io]"])

    @stub.function(
        image=image,
        schedule=modal.Cron("00 08 * * *"),
        secret=modal.Secret.from_name("id2223-project"),
        mounts=[
            modal.Mount(
                local_dir=r"C:/Users/Isac/Documents/CDATE5 ML2/ID2223/project/pandera_schemas/",
                remote_dir="/panderas_schemas",
            ),
        ],
    )
    def f():
        g()


def g():
    import datetime
    import io
    import sys
    import urllib.request

    import hopsworks
    import pandas as pd
    from pandera import Check, Column, DataFrameSchema

    start_date = str(
        (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    )

    visual_crossing_key = os.environ["VISUAL_CROSSING_KEY"]

    try:
        ResultBytes = (
            urllib.request.urlopen(
                f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Stockholm/{start_date}/{start_date}?unitGroup=metric&elements=name%2Clatitude%2Clongitude%2Ctemp%2Cwindgust%2Cwindspeed%2Cwinddir%2Ccloudcover%2Csource&include=days%2Cobs&key={visual_crossing_key}&contentType=csv"
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

    nan_cols = weather_data.columns[weather_data.isna().any()].tolist()
    if "windgust" in nan_cols and "windspeed" not in nan_cols:
        weather_data["windgust"] = weather_data["windspeed"]

        # print(nan_cols)

    weather_data = weather_data[
        ["temp", "windgust", "windspeed", "winddir", "cloudcover", "date"]
    ].copy()
    weather_data["windgust"], weather_data["windspeed"] = weather_data[
        "windgust"
    ].astype("float"), weather_data["windspeed"].astype("float")
    weather_data["temp"] = weather_data["temp"].astype("float")
    weather_data["winddir"] = weather_data["winddir"].astype("float")
    weather_data["cloudcover"] = weather_data["cloudcover"].astype("float")

    weather_data["date"] = pd.to_datetime(weather_data["date"])
    weather_data = weather_data.set_index("date")
    weather_data = weather_data.reset_index()
    weather_data["date"] = weather_data["date"].dt.strftime("%Y-%m-%d")

    schema = DataFrameSchema.from_json(
        r"/panderas_schemas/weather-feature-pipeline-daily-schema.json"
    )

    weather_data = schema.validate(weather_data)

    project = hopsworks.login()
    fs = project.get_feature_store()

    weather_fg = fs.get_or_create_feature_group(
        name="weather_data_stockholm",
        version=1,
        primary_key=["temp", "windgust", "windspeed", "winddir", "cloudcover", "date"],
        description="daily weather for Stockholm",
    )

    try:
        weather_fg.insert(weather_data, write_options={"wait_for_job": False})
    except:
        weather_data["date"] = weather_data["date"].astype("string")
        weather_fg.insert(weather_data, write_options={"wait_for_job": False})


if __name__ == "__main__":
    if LOCAL == True:
        g()
    else:
        with stub.run():
            f()
