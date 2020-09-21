import os
import uuid

from scipy.stats import randint
from sklearn import datasets
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RandomizedSearchCV

from doppel.core.context import DoppelContext


context = DoppelContext()
logger = context.get_logger()

ds = datasets.load_breast_cancer()
X = ds.data
y = ds.target


model_params = {
    'n_estimators': randint(10, 200),
    'max_depth': randint(3, 15),
    'max_features': randint(5, X.shape[1]),
    'min_samples_split': randint(1, 50),
    'min_samples_leaf': randint(1, 50)
}


while True:
    model = RandomForestClassifier()
    search = RandomizedSearchCV(model, model_params, n_iter=10, cv=10)
    search = search.fit(X, y)
    results = {
        'score': search.best_score_,
        'params': search.best_params_
    }
    logger.info('Best score = {}'.format(search.best_score_))
    filename = str(uuid.uuid4()) + '.json'
    context.save_json(results,
                      doppel_path='results/{}'.format(filename),
                      local_path=os.path.join(r'C:\data\search', filename))
