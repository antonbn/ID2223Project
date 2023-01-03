from entsoe import EntsoePandasClient
from utils.utils.keys import entsoe_key
import pandas as pd
import time
import numpy as np
from currency_converter import CurrencyConverter


client = EntsoePandasClient(api_key=entsoe_key)
c = CurrencyConverter()

date = '20221216'


start_date = pd.Timestamp(f'20221212', tz='Europe/Berlin')
end_date = pd.Timestamp(f'20221213', tz='Europe/Berlin')
country_code = 'SE_3'

day_ahead_prices = client.query_day_ahead_prices(country_code, start=start_date, end=end_date)
print(day_ahead_prices)

tax = 0.36
mean = np.mean(day_ahead_prices) / 1000
mean = c.convert(mean,'EUR','SEK') 
last = day_ahead_prices[-1] / 1000
last = c.convert(last,'EUR','SEK') 

print(f'mean: {mean}, last: {last}')

