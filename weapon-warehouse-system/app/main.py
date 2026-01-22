from fastapi import FastAPI, File, UploadFile
import pandas as pd
import numpy as np
from db import init_database, store_records

app = FastAPI()

def data_processing(file):

    # Load
    df = pd.read_csv(file)

    # Range km category
    df["risk_level"] = pd.cut(
        x=df["range_km"],
        bins=[-np.inf, 20, 100, 300, np.inf],
        labels=["low", "medium", "high", "extreme"]
    )
    # Replace None/Null in manufacturer
    df["manufacturer"].replace([None, "Null"], "Unknown")

    # Convert to dictionary
    return df.to_dict(orient="records")

@app.on_event("startup")
def startup_event():
    init_database()

@app.post("/upload")
def upload_file(file: UploadFile = File(...)):
    df = data_processing(file.file)
    store_records(df)
    file.file.close()
    return df

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

