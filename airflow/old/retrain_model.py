import pickle
import os
import pandas as pd
from catboost import CatBoostClassifier, Pool

# PATH_DATA = '../data'
# PATH_DATA = 'data'
# PATH_DATA = '/Users/sergeycherkasov/Library/CloudStorage/OneDrive-Personal/Magnit/mle-project-sprint-final/data'
# BASE_DIR = '/opt/airflow/project_root'
# PATH_DATA = os.path.join(BASE_DIR, 'data')
# PATH_MODELS = os.path.join(BASE_DIR, 'models')
# MODEL_FILE = 'retrained_model.pkl'
# RANDOM_STATE = 8310
# PATH_MODELS = '../models'
# PATH_MODELS = 'models'
# PATH_MODELS = '/Users/sergeycherkasov/Library/CloudStorage/OneDrive-Personal/Magnit/mle-project-sprint-final/models'
# MODEL_FILE = 'retrained_model.pkl'

PATH_MODELS = '/home/mle-user/mle_projects/mle-project-sprint-final/models'
MODEL_FILE = 'retrained_model.pkl'
RANDOM_STATE = 8310

candidates_for_train = pd.read_parquet(f'/home/mle-user/mle_projects/mle-project-sprint-final/data/candidates_for_train.parquet', engine='pyarrow')

features = ['categoryid_enc','parentid_enc', 'available', 'istransaction', 'day_of_week', 'day', 'hour', 'rating', 'item_id_week', 'item_viewed', 'rating_avg', 'rating_std']
cat_features = ['categoryid_enc', 'parentid_enc', 'available', 'istransaction','day_of_week', 'day', 'hour']
target = ["target"]

train_data = Pool(
    data=candidates_for_train[features],
    label=candidates_for_train[target],
    cat_features=cat_features,
)

cb_model = CatBoostClassifier(iterations=3000,
                           learning_rate=0.1,
                           loss_function='Logloss',
                           auto_class_weights='Balanced',
                           verbose=500,
                           random_seed=RANDOM_STATE)

cb_model.fit(train_data)

with open(f"{PATH_MODELS}/{MODEL_FILE}", "wb") as f:
    pickle.dump(cb_model, f)