import modal

LOCAL = False

if LOCAL == False:
    stub = modal.Stub("date-feature-pipeline-daily")
    image = modal.Image.debian_slim().pip_install(["hopsworks"])

    @stub.function(
        image=image,
        schedule=modal.Cron("01 00 * * *"),
        secret=modal.Secret.from_name("id2223-project"),
        mounts=[
            *modal.create_package_mounts(["utils"]),
            modal.Mount(
                local_dir=r"C:/Users/Isac/Documents/CDATE5 ML2/ID2223/project/utils/utils/",
                remote_dir="/",
            ),
        ],
    )
    def f():
        g()


def g():
    import datetime
    import os

    import hopsworks
    import pandas as pd

    from utils import get_dates_data

    dates_data = get_dates_data(datetime.datetime.now(), datetime.datetime.now())
    # start_date = str(datetime.datetime.now())
    # end_date = str(datetime.datetime.now())
    # date_list = pd.date_range(start=start_date, end=end_date)
    # dates_data = pd.DataFrame(data={"date": date_list})
    # dates_data["dayofyear"] = dates_data["date"].dt.dayofyear
    # dates_data["dayofweek"] = dates_data["date"].dt.dayofweek
    # dates_data["month"] = dates_data["date"].dt.month
    # dates_data["week"] = dates_data["date"].dt.isocalendar().week.astype("int64")

    # dates_data = dates_data.dropna(axis=0)
    # dates_data["date"] = pd.to_datetime(dates_data["date"])
    # dates_data = dates_data.set_index("date")
    # dates_data = dates_data.reset_index()

    # dates_data["date"] = dates_data["date"].dt.strftime("%Y-%m-%d").astype("string")

    project = hopsworks.login()
    fs = project.get_feature_store()

    date_fg = fs.get_or_create_feature_group(
        name="dates_data",
        version=1,
        primary_key=["date", "dayofyear", "dayofweek", "month", "week"],
        description="date features",
    )
    date_fg.insert(dates_data, write_options={"wait_for_job": False})


if __name__ == "__main__":
    if LOCAL == True:
        g()
    else:
        with stub.run():
            f()
