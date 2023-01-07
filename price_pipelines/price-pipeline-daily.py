import datetime
import os

import hopsworks
import modal
import pandas as pd

LOCAL = False

if LOCAL == False:
    stub = modal.Stub("price-pipeline-daily")
    image = modal.Image.debian_slim().pip_install(
        [
            "hopsworks",
            "pandas==1.5.2",
            "entsoe-py",
            "beautifulsoup4",
            "pandera[io]",
        ]
    )

    @stub.function(
        image=image,
        schedule=modal.Cron("05 01 * * *"),
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
    project = hopsworks.login()

    import requests
    from bs4 import BeautifulSoup
    from entsoe import EntsoePandasClient
    from pandera import Check, Column, DataFrameSchema

    client = EntsoePandasClient(api_key=os.environ["ENTSOE_KEY"])

    start_date = pd.Timestamp(datetime.datetime.now(), tz="Europe/Berlin")
    end_date = pd.Timestamp(
        datetime.datetime.now() + datetime.timedelta(days=1), tz="Europe/Berlin"
    )
    country_code = "SE_3"

    day_ahead_prices = client.query_day_ahead_prices(
        country_code, start=start_date, end=end_date
    )

    URL = "https://www.elbruk.se/timpriser-se3-stockholm"
    page = requests.get(URL)

    soup = BeautifulSoup(page.content, "html.parser")
    elbruk_dagspris = soup.find_all("span", class_="info-box-number")[0].text
    elbruk_dagspris = float(elbruk_dagspris.replace(",", "."))

    price_data = pd.DataFrame(
        data=[
            [
                pd.Timestamp(datetime.datetime.now(), tz="Europe/Berlin").strftime(
                    "%Y-%m-%d"
                ),
                day_ahead_prices.mean(),
                elbruk_dagspris,
            ]
        ],
        columns=["date", "entsoe_avg", "elbruk_dagspris"],
    )

    schema = DataFrameSchema.from_json(
        "/panderas_schemas/price-pipeline-daily-schema.json"
    )

    price_data = schema.validate(price_data)

    fs = project.get_feature_store()

    price_data_fg = fs.get_or_create_feature_group(
        name="price_data",
        version=1,
        primary_key="date",
        description="actual prices",
    )
    price_data_fg.insert(price_data, write_options={"wait_for_job": False})


if __name__ == "__main__":
    if LOCAL == True:
        g()
    else:
        with stub.run():
            f()
