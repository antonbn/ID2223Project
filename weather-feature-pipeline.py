from keys import visual_crossing_key
from os.path import exists
import io
import sys
import urllib.request
import pandas as pd
import hopsworks

weather_data = pd.DataFrame()

if not exists('data/weather.csv'):

	start_date = '2020-12-19'
	end_date = '2022-12-19'

	try: 
		ResultBytes = urllib.request.urlopen(f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Stockholm/{start_date}/{end_date}?unitGroup=metric&elements=name%2Clatitude%2Clongitude%2Ctemp%2Cwindgust%2Cwindspeed%2Cwinddir%2Ccloudcover%2Csource&include=days%2Cobs&key={visual_crossing_key}&contentType=csv").read().decode('UTF-8')

		weather_data = pd.read_csv(io.StringIO(ResultBytes))
		weather_data['date'] = pd.date_range(start=start_date, periods=len(weather_data), freq='D')
		print(weather_data)
		weather_data.to_csv('weather.csv')

	except urllib.error.HTTPError  as e:
		ErrorInfo= e.read().decode() 
		print('Error code: ', e.code, ErrorInfo)
		sys.exit()
	except  urllib.error.URLError as e:
		ErrorInfo= e.read().decode() 
		print('Error code: ', e.code,ErrorInfo)
		sys.exit()

else:
	weather_data = pd.read_csv('data/weather.csv')


weather_data = weather_data.dropna(axis=0)
weather_data = weather_data[['temp', 'windgust', 'windspeed', 'winddir', 'cloudcover', 'date']].copy()
weather_data['date'] = pd.to_datetime(weather_data['date'])
weather_data = weather_data.set_index('date')
weather_data = weather_data.reset_index()
weather_data['date'] = weather_data['date'].dt.strftime('%Y-%m-%d')

# print(weather_data)
project = hopsworks.login()
fs = project.get_feature_store()

weather_fg = fs.get_or_create_feature_group(
  name="weather_data_stockholm",
  version=1,
  primary_key=['temp', 'windgust', 'windspeed', 'winddir', 'cloudcover', 'date'], 
  description="daily weather for Stockholm")
weather_fg.insert(weather_data, write_options={"wait_for_job" : False})