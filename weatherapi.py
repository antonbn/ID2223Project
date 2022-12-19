
import codecs
import csv
import io
import json
import sys
import urllib.request

import pandas as pd
import requests

startdate = '2020-12-19'
enddate = '2020-12-24'

try: 
  ResultBytes = urllib.request.urlopen(f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Stockholm/{startdate}/{enddate}?unitGroup=metric&elements=name%2Clatitude%2Clongitude%2Ctemp%2Cwindgust%2Cwindspeed%2Cwinddir%2Ccloudcover%2Csource&include=days%2Cobs&key=KLGY9GAYT6X4EH3XBSKMX6ZFE&contentType=csv").read().decode('UTF-8')
 
  df = pd.read_csv(io.StringIO(ResultBytes))
  df['date'] = pd.date_range(start=startdate, periods=len(df), freq='D')
  print(df)

except urllib.error.HTTPError  as e:
  ErrorInfo= e.read().decode() 
  print('Error code: ', e.code, ErrorInfo)
  sys.exit()
except  urllib.error.URLError as e:
  ErrorInfo= e.read().decode() 
  print('Error code: ', e.code,ErrorInfo)
  sys.exit()