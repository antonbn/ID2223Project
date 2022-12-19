import io
import sys
import urllib.request
import pandas as pd
from keys import visual_crossing_key

start_date = '2020-12-19'
end_date = '2022-12-19'

try: 
  ResultBytes = urllib.request.urlopen(f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Stockholm/{start_date}/{end_date}?unitGroup=metric&elements=name%2Clatitude%2Clongitude%2Ctemp%2Cwindgust%2Cwindspeed%2Cwinddir%2Ccloudcover%2Csource&include=days%2Cobs&key={visual_crossing_key}&contentType=csv").read().decode('UTF-8')
 
  df = pd.read_csv(io.StringIO(ResultBytes))
  df['date'] = pd.date_range(start=start_date, periods=len(df), freq='D')
  print(df)
  df.to_csv('weather.csv')

except urllib.error.HTTPError  as e:
  ErrorInfo= e.read().decode() 
  print('Error code: ', e.code, ErrorInfo)
  sys.exit()
except  urllib.error.URLError as e:
  ErrorInfo= e.read().decode() 
  print('Error code: ', e.code,ErrorInfo)
  sys.exit()