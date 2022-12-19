from entsoe import EntsoePandasClient
from keys import entsoe_key
import pandas as pd
import time

client = EntsoePandasClient(api_key=entsoe_key)

start_date = pd.Timestamp('20201219', tz='Europe/Berlin')
end_date = pd.Timestamp('20221219', tz='Europe/Berlin')
country_code = 'SE_3'

batch_end_date = start_date + pd.Timedelta(days=100)

energy_data = pd.DataFrame()

while start_date < end_date:
	print(f'{start_date} - {batch_end_date}')

	day_ahead_prices = client.query_day_ahead_prices(country_code, start=start_date, end=batch_end_date)
	load = client.query_load(country_code, start=start_date, end=batch_end_date)
	aggregate_water_reservoirs_and_hydro_storage = client.query_aggregate_water_reservoirs_and_hydro_storage(
		country_code, start=start_date, end=batch_end_date)

	result = pd.concat([day_ahead_prices, load, aggregate_water_reservoirs_and_hydro_storage], axis=1)
	result = result.ffill()

	energy_data = pd.concat([energy_data, result], axis=0)

	start_date = batch_end_date + pd.Timedelta(days=1)
	batch_end_date = start_date + pd.Timedelta(days=100)
	if batch_end_date > end_date:
		batch_end_date = end_date

	time.sleep(61)

energy_data.to_csv(f'energy.csv')
