import datetime

import hopsworks
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px

import streamlit as st

from utils import eur_sek_convert

st.markdown(
    """**Logging of predictions of daily average energy prices in Stockholm/SE3 for the upcoming 7 days**"""
)
progressBar = st.progress(0)
progressBar.progress(20)

project = hopsworks.login()
fs = project.get_feature_store()

price_pred_fg = fs.get_feature_group(name="price_predictions", version=1).read()

pric_actual_fg = fs.get_feature_group(name="price_data", version=1).read()

price_combined = pric_actual_fg.merge(price_pred_fg, on="date")
price_combined["predicted_price"] = eur_sek_convert(price_combined["predicted_price"])
price_combined["entsoe_avg"] = eur_sek_convert(price_combined["entsoe_avg"])
progressBar.progress(60)
# print(price_combined)

total_mae_entsoe = (
    sum(abs(price_combined["entsoe_avg"] - price_combined["predicted_price"]))
    / price_combined.shape[0]
)

total_mae_elbruk = (
    sum(abs(price_combined["elbruk_dagspris"] - price_combined["predicted_price"]))
    / price_combined.shape[0]
)

total_rmse_entsoe = (
    np.sqrt(sum(np.power((price_combined["entsoe_avg"] - price_combined["predicted_price"]),2))
    / price_combined.shape[0])
)

total_rmse_elbruk = (
    np.sqrt(sum(np.power((price_combined["elbruk_dagspris"] - price_combined["predicted_price"]),2))
    / price_combined.shape[0])
)

days_ahead_mae_entsoe = [
    price_combined.query(f"days_ahead == {i}") for i in range(1, 8)
]
days_ahead_mae_entsoe = [df for df in days_ahead_mae_entsoe if not df.empty]
days_ahead_mae_entsoe = [
    sum(abs(df["entsoe_avg"] - df["predicted_price"])) / df.shape[0]
    for df in days_ahead_mae_entsoe
]

days_ahead_mae_elbruk = [
    price_combined.query(f"days_ahead == {i}") for i in range(1, 8)
]
days_ahead_mae_elbruk = [df for df in days_ahead_mae_elbruk if not df.empty]
days_ahead_mae_elbruk = [
    sum(abs(df["elbruk_dagspris"] - df["predicted_price"])) / df.shape[0]
    for df in days_ahead_mae_elbruk
]

days_ahead_rmse_entsoe = [
    price_combined.query(f"days_ahead == {i}") for i in range(1, 8)
]
days_ahead_rmse_entsoe = [df for df in days_ahead_rmse_entsoe if not df.empty]
days_ahead_rmse_entsoe = [
    np.sqrt(sum(np.power((df["entsoe_avg"] - df["predicted_price"]),2)) / df.shape[0])
    for df in days_ahead_rmse_entsoe
]

days_ahead_rmse_elbruk = [
    price_combined.query(f"days_ahead == {i}") for i in range(1, 8)
]
days_ahead_rmse_elbruk = [df for df in days_ahead_rmse_elbruk if not df.empty]
days_ahead_rmse_elbruk = [
    np.sqrt(sum(np.power((df["elbruk_dagspris"] - df["predicted_price"]),2)) / df.shape[0])
    for df in days_ahead_rmse_elbruk
]

progressBar.progress(70)
columns = ["total MAE"] + [f"d + {i} MAE" for i in range(1, 8)]
entsoe_mae_data = [total_mae_entsoe] + [mae for mae in days_ahead_mae_entsoe]
entsoe_mae_data += ["NaN"] * (8 - len(entsoe_mae_data))
entsoe_mae_data = [entsoe_mae_data]

entsoe_mae = pd.DataFrame(data=entsoe_mae_data, columns=columns)


elbruk_mae_data = [total_mae_elbruk] + [mae for mae in days_ahead_mae_elbruk]
elbruk_mae_data += ["NaN"] * (8 - len(elbruk_mae_data))
elbruk_mae_data = [elbruk_mae_data]

elbruk_mae = pd.DataFrame(data=elbruk_mae_data, columns=columns)

st.markdown("""**ENTSOE daily average price MAE (SEK ÖRE)**""")
st.dataframe(data=entsoe_mae.style.background_gradient(axis="columns", cmap="YlOrRd"))
st.markdown("""**elbruk.se dagspris MAE (SEK ÖRE)**""")
st.dataframe(data=elbruk_mae.style.background_gradient(axis="columns", cmap="YlOrRd"))

#####

columns = ["total RMSE"] + [f"d + {i} RMSE" for i in range(1, 8)]
entsoe_rmse_data = [total_rmse_entsoe] + [rmse for rmse in days_ahead_rmse_entsoe]
entsoe_rmse_data += ["NaN"] * (8 - len(entsoe_rmse_data))
entsoe_rmse_data = [entsoe_rmse_data]

entsoe_rmse = pd.DataFrame(data=entsoe_rmse_data, columns=columns)


elbruk_rmse_data = [total_rmse_elbruk] + [rmse for rmse in days_ahead_rmse_elbruk]
elbruk_rmse_data += ["NaN"] * (8 - len(elbruk_rmse_data))
elbruk_rmse_data = [elbruk_rmse_data]

elbruk_rmse = pd.DataFrame(data=elbruk_rmse_data, columns=columns)

st.markdown("""**ENTSOE daily average price RMSE (SEK ÖRE)**""")
st.dataframe(data=entsoe_rmse.style.background_gradient(axis="columns", cmap="YlOrRd"))
st.markdown("""**elbruk.se dagspris RMSE (SEK ÖRE)**""")
st.dataframe(data=elbruk_rmse.style.background_gradient(axis="columns", cmap="YlOrRd"))

progressBar.progress(80)

latest_day = price_combined[price_combined["date"] == price_combined["date"].max()]

latest_day_latest_pred = latest_day[
    latest_day["days_ahead"] == latest_day["days_ahead"].min()
]

dates = [latest_day_latest_pred["date"].values[0]]
predicted_price = [latest_day_latest_pred["predicted_price"].values[0]]
entsoe = [latest_day_latest_pred["entsoe_avg"].values[0]]
entsoe_error = [
    latest_day_latest_pred["predicted_price"].values[0]
    - latest_day_latest_pred["entsoe_avg"].values[0]
]
elbruk = [latest_day_latest_pred["elbruk_dagspris"].values[0]]
elbruk_error = [
    latest_day_latest_pred["predicted_price"].values[0]
    - latest_day_latest_pred["elbruk_dagspris"].values[0]
]
days_ahead = [latest_day_latest_pred["days_ahead"].values[0]]


last_day = datetime.datetime.strptime(dates[0], "%Y-%m-%d")

for i in range(1, 10):
    prev_day = last_day - datetime.timedelta(days=i)
    if (price_combined["date"] == prev_day.strftime("%Y-%m-%d")).any():

        prev_day_df = price_combined[
            price_combined["date"] == prev_day.strftime("%Y-%m-%d")
        ].sort_values(by=["days_ahead"])
        prev_day_df_latest_pred = prev_day_df[
            prev_day_df["days_ahead"] == prev_day_df["days_ahead"].min()
        ]
        for i, row in prev_day_df.iterrows():
            dates.append(row["date"])
            predicted_price.append(row["predicted_price"])
            entsoe.append(row["entsoe_avg"])
            entsoe_error.append(row["predicted_price"] - row["entsoe_avg"])
            elbruk.append(row["elbruk_dagspris"])
            elbruk_error.append(row["predicted_price"] - row["elbruk_dagspris"])
            days_ahead.append(row["days_ahead"])

data = [dates, days_ahead, predicted_price, entsoe, entsoe_error, elbruk, elbruk_error]
data = map(list, zip(*data))
progressBar.progress(90)
# print(data)
last_n_results = pd.DataFrame(
    data=data,
    columns=[
        "date",
        "days ahead",
        "predicted price",
        "entsoe price",
        "predicted - entsoe ",
        "elbruk price",
        "predicted - elbruk",
    ],
)
# print(last_n_results)
st.markdown("""**Detailed logging for latest 10 days**""")
st.dataframe(data=last_n_results.style)
progressBar.progress(100)
