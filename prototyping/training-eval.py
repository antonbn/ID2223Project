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
    feature_view = fs.get_feature_view(name="energy_weather_date", version=1)
  
except:
    weather_fg = fs.get_or_create_feature_group(name = 'weather_data_stockholm', version=1)
    energy_fg = fs.get_or_create_feature_group(name = 'energy_prices', version=1)
    dates_fg = fs.get_or_create_feature_group(name = 'dates_data', version=1)
    query = weather_fg.select_all().join(energy_fg.select_all()).join(dates_fg.select_all())
    query.read()
    print(query.show(10))

    feature_view = fs.create_feature_view(name="energy_weather_date",
                                        version=1,
                                        description="Energy and weather and date data for Stockholm",
                                        labels=["price"],
                                        query=query)    
    feature_view.create_training_data()
    # feature_view = feature_view.sort_values(by=["date"], ascending=True).reset_index(drop=True)

train_data, prices = feature_view.get_training_data(1)[0], feature_view.get_training_data(1)[1]
train_data = train_data.set_index('date')
print(train_data.head(10))

# c = CurrencyConverter()
# def currconvert(row):
#     return (c.convert(row,'EUR','SEK') / 1000) * 100 

# train_data['p_1'] = train_data['p_1'].apply(currconvert)
# train_data['p_2'] = train_data['p_2'].apply(currconvert)
# train_data['p_3'] = train_data['p_3'].apply(currconvert)
# train_data['p_4'] = train_data['p_4'].apply(currconvert)
# train_data['p_5'] = train_data['p_5'].apply(currconvert)
# train_data['p_6'] = train_data['p_6'].apply(currconvert)
# train_data['p_7'] = train_data['p_7'].apply(currconvert)
# train_data['p_1'] = train_data.apply(lambda row: (c.convert(row['p_1'],'EUR','SEK') / 1000) * 100)
# train_data['p_2'] = train_data.apply(lambda row: (c.convert(row['p_2'],'EUR','SEK') / 1000) * 100)
# train_data['p_3'] = train_data.apply(lambda row: (c.convert(row['p_3'],'EUR','SEK') / 1000) * 100)
# train_data['p_4'] = train_data.apply(lambda row: (c.convert(row['p_4'],'EUR','SEK') / 1000) * 100)
# train_data['p_5'] = train_data.apply(lambda row: (c.convert(row['p_5'],'EUR','SEK') / 1000) * 100)
# train_data['p_6'] = train_data.apply(lambda row: (c.convert(row['p_6'],'EUR','SEK') / 1000) * 100)
# train_data['p_7'] = train_data.apply(lambda row: (c.convert(row['p_7'],'EUR','SEK') / 1000) * 100)

# train_data = train_data.sort_values(by=["date"], ascending=True).reset_index(drop=True)
# prices = feature_view.get_training_data(1)[1]
# print(prices)
# print(train_data.head(10))

# model = XGBRegressor(subsample=0.8, n_estimators=1000,max_depth=3, learning_rate=0.1, colsample_bytree=0.8,
# colsample_bylevel=0.7)
model = XGBRegressor( n_estimators=600,max_depth=5, learning_rate=0.2, min_child_weight=5)
X, y = train_data, prices


# print('type y: ', type(y))


# y = y.apply(currconvert, axis=1)
 
# cv = RepeatedKFold(n_splits=10, n_repeats=3, random_state=1)
# scores = cross_val_score(model, X, y, scoring='neg_mean_absolute_error', cv=cv, n_jobs=-1)
# scores = absolute(scores)
# print('Mean MAE: %.3f (%.3f)' % (scores.mean(), scores.std()) )

# params = { 'objective' : ['reg:squarederror', 'reg:squaredlogerror','reg:logistic','reg:pseudohubererror','reg:absoluteerror'],
#             'max_depth': [3,4, 5, 6, 10, 15, 20],
#             'min_child_weight': [1,2,3,4,5,6,7,8,9]}

# params = { 'max_depth': [3,4, 5, 6,7,8,9, 10, 15, 20],
#             'min_child_weight': [1,2,3,4,5,6,7,8,9],
#             'learning_rate': [0.01, 0.1, 0.2, 0.3],
#             'n_estimators': [100, 500, 1000]}

# params = { 'max_depth': [4, 5, 6,7,8,9, 10, 15, 20],
#             'min_child_weight': [3,4,5,6,7,8,9],
#             'learning_rate': [0.1,0.15, 0.2,0.25, 0.3],
#             'n_estimators': [300,400,500,600,700 ]}


# # params = { 'objective' : ['reg:squarederror', 'reg:squaredlogerror','reg:logistic','reg:pseudohubererror']
# #     'max_depth': [3, 5, 6, 10, 15, 20],
# #            'learning_rate': [0.01, 0.1, 0.2, 0.3],
# #            'subsample': np.arange(0.5, 1.0, 0.1),
# #            'colsample_bytree': np.arange(0.4, 1.0, 0.1),
# #            'colsample_bylevel': np.arange(0.4, 1.0, 0.1),
# #            'n_estimators': [100, 500, 1000]}           
# xgbr = xgb.XGBRegressor(seed = 20)
# clf = RandomizedSearchCV(estimator=xgbr,
#                          param_distributions=params,
#                          scoring='neg_mean_squared_error',
#                          n_iter=50,
#                          verbose=1)

# clf.fit(X, y)
# print("Best parameters:", clf.best_params_)
# print("Lowest RMSE: ", (-clf.best_score_)**(1/2.0))

