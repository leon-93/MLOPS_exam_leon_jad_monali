import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV, KFold
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score, roc_auc_score
from sklearn.preprocessing import OrdinalEncoder
import mlflow

# Configuration de l'URI du serveur MLflow
remote_server_uri = "http://127.0.0.1:5000"
mlflow.set_tracking_uri(remote_server_uri)

# Définition de l'expérience MLflow
mlflow.set_experiment("experiment_02")

# Activation du suivi automatique des métriques et des paramètres avec MLflow
mlflow.sklearn.autolog()

# Chargement des données
input_file = "./extracted_features.csv"
data = pd.read_csv(input_file)

# Handling missing values before any further processing
for column in data.columns:
    if data[column].dtype == 'object':
        # Fill categorical with the most frequent value (mode)
        data[column].fillna(data[column].mode()[0], inplace=True)
    else:
        # Fill numeric with median
        data[column].fillna(data[column].median(), inplace=True)

# Replace infinite values if they exist
data.replace([np.inf, -np.inf], np.nan, inplace=True)
data.fillna(data.median(), inplace=True)

# Identifier les fonctionnalités catégorielles
categorical_features = data.select_dtypes(include=["object"]).columns.tolist()
print("Categorical Features:", categorical_features)

# Encodage des fonctionnalités catégorielles
ordinal_encoder = OrdinalEncoder()
data[categorical_features] = ordinal_encoder.fit_transform(data[categorical_features])

# Séparation des fonctionnalités et de la cible
X = data.drop(columns=["Etiquette_DPE"])  # Features
y = data["Etiquette_DPE"]  # Target variable

# Split des données en ensembles d'entraînement et de test
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=808
)

# Initialisation du modèle
rf = RandomForestClassifier()

# Définition de la grille des paramètres à rechercher
param_grid = {
    "n_estimators": [100, 200, 300],
    "max_depth": [None, 10, 20],
    "min_samples_leaf": [1, 5, 10],
}

# Configuration de la validation croisée avec GridSearchCV
cv = KFold(n_splits=3, random_state=84, shuffle=True)
grid_search = GridSearchCV(
    estimator=rf, param_grid=param_grid, cv=cv, scoring="accuracy", verbose=1
)

# Entraînement du modèle sur l'ensemble d'entraînement
try:
    grid_search.fit(X_train, y_train)
except ValueError as e:
    print(f"Error during model fitting: {e}")

# Meilleurs paramètres et meilleur score
print(f"Best parameters: {grid_search.best_params_}")
print(f"Best cross-validation score: {grid_search.best_score_}")
print(f"Best model: {grid_search.best_estimator_}")

# Évaluation sur l'ensemble de test
y_pred = grid_search.predict(X_test)
print(classification_report(y_test, y_pred))

# Enregistrement des prédictions dans un DataFrame
predictions = pd.DataFrame()
predictions["y_true"] = y_test.values
predictions["y_pred"] = y_pred

# Enregistrement du modèle et des métriques dans MLflow
with mlflow.start_run() as run:
    mlflow.sklearn.log_model(grid_search.best_estimator_, "random_forest_model")
    mlflow.log_params(grid_search.best_params_)
    mlflow.log_metric("accuracy", accuracy_score(y_test, y_pred))
    mlflow.log_metric("roc_auc", roc_auc_score(y_test, y_pred))