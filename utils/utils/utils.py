def eur_sek_convert(val):
    from forex_python.converter import CurrencyRates
    c = CurrencyRates()
    return ((c.convert('EUR','SEK',val) / 1000) * 100)

def start_end_date(start_offset, end_offset):
    import datetime
    import pandas as pd

    start_date = pd.Timestamp(
        datetime.datetime.now() + datetime.timedelta(days=start_offset), tz="Europe/Berlin"
    )
    end_date = pd.Timestamp(
        datetime.datetime.now() + datetime.timedelta(days=end_offset), tz="Europe/Berlin"
    )

    return start_date, end_date

def get_dates_data(start_date, end_date):
    import pandas as pd
    start_date = str(start_date)
    end_date = str(end_date)
    date_list = pd.date_range(start=start_date, end=end_date)
    dates_data = pd.DataFrame(data={"date": date_list})
    dates_data["dayofyear"] = dates_data["date"].dt.dayofyear
    dates_data["dayofweek"] = dates_data["date"].dt.dayofweek
    dates_data["month"] = dates_data["date"].dt.month
    dates_data["week"] = dates_data["date"].dt.isocalendar().week.astype("int64")

    dates_data = dates_data.dropna(axis=0)
    dates_data["date"] = pd.to_datetime(dates_data["date"])
    dates_data = dates_data.set_index("date")
    dates_data = dates_data.reset_index()

    dates_data["date"] = dates_data["date"].dt.strftime("%Y-%m-%d")
    return dates_data