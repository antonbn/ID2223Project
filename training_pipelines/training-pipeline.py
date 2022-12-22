import os
import modal

LOCAL=True

if LOCAL == False:
   stub = modal.Stub()
   image = modal.Image.debian_slim().apt_install(["libgomp1"]).pip_install(["hopsworks", "seaborn", "joblib", "scikit-learn"])

   @stub.function(image=image, schedule=modal.Period(days=1), secret=modal.Secret.from_name("id2223-lab1"))
   def train_modal():
       train_fn()

def train_fn():
    import hopsworks
    from xgboost import XGBRegressor
    import joblib
    from hsml.schema import Schema
    from hsml.model_schema import ModelSchema
    from sklearn.metrics import mean_absolute_error


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

    # You can read training data, randomly split into train/test sets of features (X) and labels (y)        
    X_train, X_test, y_train, y_test = feature_view.train_test_split(0.05)
    X_train, X_test = X_train.set_index('date'), X_test.set_index('date')


    model = XGBRegressor( n_estimators=600,max_depth=5, learning_rate=0.2, min_child_weight=5)

    model.fit(X_train, y_train)

    # Evaluate model performance using the features from the test set (X_test)
    # print('xtest: ', X_test)
    y_pred = model.predict(X_test)

    # Compare predictions (y_pred) with the labels in the test set (y_test)
    mae = mean_absolute_error(y_test, y_pred, multioutput='uniform_average')

    # We will now upload our model to the Hopsworks Model Registry. First get an object for the model registry.
    mr = project.get_model_registry()

    # The contents of the 'iris_model' directory will be saved to the model registry. Create the dir, first.
    model_dir="price_model"
    if os.path.isdir(model_dir) == False:
        os.mkdir(model_dir)

    # Save both our model and the confusion matrix to 'model_dir', whose contents will be uploaded to the model registry
    joblib.dump(model, model_dir + "/price_model.pkl")

    # Specify the schema of the model's input/output using the features (X_train) and labels (y_train)
    input_schema = Schema(X_train)
    output_schema = Schema(y_train)
    model_schema = ModelSchema(input_schema, output_schema)

    # Create an entry in the model registry that includes the model's name, desc, metrics
    price_model = mr.python.create_model(
        name="price_modal", 
        metrics={"accuracy" : mae},
        model_schema=model_schema,
        description="electricity price predictor"
    )

    # Upload the model to the model registry, including all files in 'model_dir'
    price_model.save(model_dir)

if __name__ == "__main__":
    if LOCAL == True :
        train_fn()
    else:
        with stub.run():
            train_modal()