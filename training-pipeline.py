import xgboost as xgb
import hopsworks
import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import RepeatedKFold
from xgboost import XGBRegressor
from numpy import absolute
from currency_converter import CurrencyConverter


project = hopsworks.login()
fs = project.get_feature_store()

try: 
    feature_view = fs.get_feature_view(name="energy_weather", version=1)
except:
    weather_fg = fs.get_or_create_feature_group(name = 'weather_data_stockholm', version=1)
    energy_fg = fs.get_or_create_feature_group(name = 'energy_prices', version=1)
    query = weather_fg.select_all().join(energy_fg.select_all())
    # query.read()
    # print(query.show(10))

    feature_view = fs.create_feature_view(name="energy_weather",
                                        version=1,
                                        description="Energy and weather data for Stockholm",
                                        labels=["price"],
                                        query=query)    
    # feature_view = feature_view.sort_values(by=["date"], ascending=True).reset_index(drop=True)

train_data = feature_view.get_training_data(1)[0]
train_data = train_data.set_index('date')

# train_data = train_data.sort_values(by=["date"], ascending=True).reset_index(drop=True)
prices = feature_view.get_training_data(1)[1]
print(prices)
print(train_data.head(30))

model = XGBRegressor()
X, y = train_data, prices

c = CurrencyConverter()
print('type y: ', type(y))

def currconvert(row):
    return c.convert(row,'EUR','SEK') * 1000 
y = y.apply(currconvert, axis=1)
 
cv = RepeatedKFold(n_splits=10, n_repeats=1, random_state=1)
scores = cross_val_score(model, X, y, scoring='neg_mean_absolute_error', cv=cv, n_jobs=-1)
scores = absolute(scores)
print('Mean MAE: %.3f (%.3f)' % (scores.mean(), scores.std()) )