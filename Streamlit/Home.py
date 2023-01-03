import datetime

import hopsworks
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px

import streamlit as st

st.markdown(
    """Predictions of daily average energy prices in Stockholm/SE3 for the upcoming 7 days."""
)

progressBar = st.progress(0)
progressBar.progress(20)


def get_price():
    from utils import eur_sek_convert

    project = hopsworks.login()
    fs = project.get_feature_store()

    price_pred_fg = fs.get_feature_group(name="price_predictions", version=1).read()

    dates = [
        (datetime.datetime.now() + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(1, 8)
    ]
    days_ahead = list(range(1, 8))

    prices = [
        price_pred_fg.loc[
            (price_pred_fg["date"] == dates[i])
            & (price_pred_fg["days_ahead"] == days_ahead[i])
        ]["predicted_price"].values[0]
        for i in range(0, 7)
    ]
    price_predictions = pd.DataFrame()
    price_predictions["date"] = dates
    price_predictions["predicted price (SEK öre)"] = prices

    # def currconvert(row):
    #     return round(((c.convert('EUR','SEK',row) / 1000) * 100),2)
    # price_predictions['predicted price (SEK öre)'] = price_predictions['predicted price (SEK öre)'].apply(currconvert)
    price_predictions["predicted price (SEK öre)"] = round(
        eur_sek_convert(price_predictions["predicted price (SEK öre)"]), 2
    )
    # print(price_predictions)
    return price_predictions


prices = get_price()
progressBar.progress(70)

fig = px.line(prices, x="date", y="predicted price (SEK öre)")
fig.update_yaxes(rangemode="tozero")

prices = prices.set_index("date")
prices = prices.reset_index()
prices.index = np.arange(1, len(prices) + 1)

st.write(fig)
st.dataframe(data=prices.style.background_gradient(cmap="YlOrRd"))
progressBar.progress(100)
