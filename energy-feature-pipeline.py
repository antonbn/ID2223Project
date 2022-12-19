from entsoe import EntsoePandasClient
from keys import entsoe_key
import pandas as pd
import time

client = EntsoePandasClient(api_key=entsoe_key)

start_date = pd.Timestamp('20201219', tz='Europe/Berlin')
end_date = pd.Timestamp('20221219', tz='Europe/Berlin')
country_code = 'SE_3'

batch_end_date = start_date + pd.Timedelta(days=300)

i = 0

while start_date < end_date:
	result = client.query_day_ahead_prices(country_code, start=start_date, end=batch_end_date)

	start_date = batch_end_date + pd.Timedelta(days=1)
	batch_end_date = start_date + pd.Timedelta(days=300)
	if batch_end_date > end_date:
		batch_end_date = end_date

	time.sleep(61)

	result.to_csv(f'energy_{i}.csv')
	i += 1


