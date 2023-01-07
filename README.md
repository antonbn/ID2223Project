# ID2223Project

## Overview of Prediction Service

The [prediction service](https://isaclorentz-id2223proj-streamlit-2-streamlithome-39q64f.streamlit.app/) predicts the daily average energy price in Stockholm/SE3 for the upcoming 7 days. The service is of value to people living in the SE3 elområde and who want to know the electricity prices for the upcoming week.

## Model

XGBoostRegressor, which we performed hyperparameter tuning on.

## Data Sources

The [entsoe API](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html) (Transparency Platform RESTful API) is used to fetch data about energy prices, load, and aggregated filling rate of water reservoirs and hydro storage plants. For this we used the [entsoe-py](https://github.com/EnergieID/entsoe-py) client. 

[Visual crossing weather API](https://www.visualcrossing.com/weather-api) is used to featch weater data, including: temperature, wind gust, wind speed, wind direction, and cloud cover

[elbruk.se](https://www.elbruk.se) dagspris is scraped daily through beautifulsoup and a modal job

EUR and SEK currency rates are collected from a [python library](https://github.com/MicroPyramid/forex-python) that get daily rates from the European Central Bank.

The date features need no external data source.

## Motivation Why The Project Deserves Excellent

- The prediction service solves a useful problem
- We've added a [detailed monitoring page](https://isaclorentz-id2223proj-streamlit-2-streamlithome-39q64f.streamlit.app/Monitoring) for the prediction service
- We've used data validation using Pandera to validate the data before adding it to the feature groups
- Reused code is in the utils module to follow best MLOps practices. Data validation schemas are read from file to avoid code duplication and training/serving drift.