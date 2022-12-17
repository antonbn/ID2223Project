
import urllib.request
import sys
import csv
import codecs
import json
import pandas as pd
import requests
import io

try: 
  ResultBytes = urllib.request.urlopen("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Stockholm/2022-12-08/2022-12-15?unitGroup=metric&elements=name%2Clatitude%2Clongitude%2Ctemp%2Cwindgust%2Cwindspeed%2Cwinddir%2Ccloudcover%2Csource&include=days%2Cobs&key=KLGY9GAYT6X4EH3XBSKMX6ZFE&contentType=csv").read().decode('UTF-8')
 
  df = pd.read_csv(io.StringIO(ResultBytes), header=None)
  #print(df)

except urllib.error.HTTPError  as e:
  ErrorInfo= e.read().decode() 
  print('Error code: ', e.code, ErrorInfo)
  sys.exit()
except  urllib.error.URLError as e:
  ErrorInfo= e.read().decode() 
  print('Error code: ', e.code,ErrorInfo)
  sys.exit()