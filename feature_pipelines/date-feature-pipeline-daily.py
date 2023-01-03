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
