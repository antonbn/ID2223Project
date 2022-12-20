import hopsworks
import pandas as pd
import xgboost as xgb
from currency_converter import CurrencyConverter
from numpy import absolute
from sklearn.model_selection import RepeatedKFold, cross_val_score
from xgboost import XGBRegressor
import numpy as np
from sklearn.model_selection import RandomizedSearchCV

project = hopsworks.login()
fs = project.get_feature_store()

try: 
    feature_view = fs.get_feature_view(name="energy_weather", version=1)
    # feature_view.create_training_data()
except:
    weather_fg = fs.get_or_create_feature_group(name = 'weather_data_stockholm', version=1)
    energy_fg = fs.get_or_create_feature_group(name = 'energy_prices', version=1)
    query = weather_fg.select_all().join(energy_fg.select_all())
    query.read()
    print(query.show(10))

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
# print(prices)
# print(train_data.head(10))

model = XGBRegressor(subsample=0.8, n_estimators=1000,max_depth=3, learning_rate=0.1, colsample_bytree=0.8,
colsample_bylevel=0.7)
X, y = train_data, prices

c = CurrencyConverter()
# print('type y: ', type(y))

def currconvert(row):
    return (c.convert(row,'EUR','SEK') / 1000) * 100 
y = y.apply(currconvert, axis=1)
 
cv = RepeatedKFold(n_splits=10, n_repeats=3, random_state=1)
scores = cross_val_score(model, X, y, scoring='neg_mean_absolute_error', cv=cv, n_jobs=-1)
scores = absolute(scores)
print('Mean MAE: %.3f (%.3f)' % (scores.mean(), scores.std()) )

# params = { 'max_depth': [3, 5, 6, 10, 15, 20],
#            'learning_rate': [0.01, 0.1, 0.2, 0.3],
#            'subsample': np.arange(0.5, 1.0, 0.1),
#            'colsample_bytree': np.arange(0.4, 1.0, 0.1),
#            'colsample_bylevel': np.arange(0.4, 1.0, 0.1),
#            'n_estimators': [100, 500, 1000]}
# xgbr = xgb.XGBRegressor(seed = 20)
# clf = RandomizedSearchCV(estimator=xgbr,
#                          param_distributions=params,
#                          scoring='neg_mean_squared_error',
#                          n_iter=30,
#                          verbose=1)

# clf.fit(X, y)
# print("Best parameters:", clf.best_params_)
# print("Lowest RMSE: ", (-clf.best_score_)**(1/2.0))