import datetime
import os

import hopsworks
import modal
import pandas as pd

LOCAL = True

if LOCAL == False:
    stub = modal.Stub("price-pipeline-daily")
    image = modal.Image.debian_slim().pip_install(
        [
            "hopsworks",
            "pandas==1.5.2",
            "entsoe-py",
            "beautifulsoup4",
        ]
    )

    @stub.function(
        image=image,
        schedule=modal.Cron("05 01 * * *"),
        secret=modal.Secret.from_name("id2223-project"),
    )
    def f():
        g()


def g():
    from forex_python.converter import CurrencyRates
    from pandera import Check, Column, DataFrameSchema

    c = CurrencyRates()
    project = hopsworks.login()

    elbruk = pd.read_csv('../data/elbruk_backfill.csv')
    entsoe = pd.read_csv('../data/entsoe_backfill_daily.csv')    

    combined = entsoe.merge(elbruk, on='date')
    combined = combined.loc[:, ~combined.columns.str.contains('^Unnamed')]
    schema = DataFrameSchema(
        {
        "date": Column(
            checks=[
                Check(
                    lambda d: bool(datetime.datetime.strptime(d, "%Y-%m-%d")),
                    element_wise=True,
                ),
            ]
        ),
        "entsoe_avg": Column(float),
        "elbruk_dagspris": Column(float),
    }
    )
    combined = schema.validate(combined)
    schema.to_json("../pandera_schemas/price-pipeline-daily-schema.json")

    fs = project.get_feature_store()

    price_data_fg = fs.get_feature_group(
        name="price_data",
        version=1,
    )
    price_data_fg.insert(combined, write_options={"wait_for_job": False})


if __name__ == "__main__":
    if LOCAL == True:
        g()
    else:
        with stub.run():
            f()
