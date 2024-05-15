from fastapi import FastAPI
from pydantic import BaseModel
import mlflow.sklearn
import mlflow
import pandas as pd
import typing as t

#Set the MLflow tracking URI and experiment
mlflow.set_tracking_uri("http://127.0.0.1:5000")
mlflow.set_experiment("dpe_forever")

#Initialize the FastAPI app
app = FastAPI()

#Function to get the best run ID
def get_run_id():
    runs = mlflow.search_runs(order_by=["metrics.best_cv_score desc"], max_results=1)
    best_run = runs.head(1).to_dict(orient="records")[0]
    return best_run["run_id"]

#Function to load the model
def get_model():
    run_id = get_run_id()
    model_uri = f"runs:/{run_id}/best_estimator"
    model = mlflow.pyfunc.load_model(model_uri)
    return model

#Function to load sample data
def load_data(filename: str = "sample.json") -> t.Dict:
    print("Loading sample data from ", filename)
    data = pd.read_json(filename)
    return data.to_dict(orient="records")[0]

#Pydantic model for prediction input
class PredictionInput(BaseModel):
    Etiquette_DPE: str
    Etiquette_GES: str
    Type_bâtiment: str
    Surface_habitable_logement: float
    Conso_5_usages_finale: float
    Conso_chauffage_finale: float
    Conso_ECS_finale: float
    Emission_GES_5_usages: float
    Emission_GES_chauffage: float

#POST query to endpoint for prediction
@app.post("/predict/")
def predict(input: PredictionInput):
    model = get_model()

    # Load sample data
    data = load_data()
    # Update sample data with input values
    data.update(input.dict())

    # Convert data to DataFrame
    data_df = pd.DataFrame([data])

    # Make prediction
    prediction = model.predict(data_df)

    return {"prediction": int(prediction)}
