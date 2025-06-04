#! /usr/bin/env python
# -*- coding: utf-8 -*-
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime
import structlog
from src.model.churn_predictor import ChurnPredictor
from src.storage.sql_server_manager import SQLServerManager

logger = structlog.get_logger()

app = FastAPI(
    title="Churn Prediction API",
    description="API for customer churn predictions",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global model instance
predictor = None
sql_manager = SQLServerManager()

class CustomerFeatures(BaseModel):
    customer_id: str = Field(..., description="Unique customer identifier")
    total_events: int = Field(ge=0, description="Total number of events")
    unique_event_types: int = Field(ge=0, description="Number of unique event types")
    purchase_count: int = Field(ge=0, description="Number of purchases")
    page_view_count: int = Field(ge=0, description="Number of page views")
    support_ticket_count: int = Field(ge=0, description="Number of support tickets")
    avg_purchase_amount: float = Field(ge=0, description="Average purchase amount")
    total_purchase_amount: float = Field(ge=0, description="Total purchase amount")
    days_since_last_activity: int = Field(ge=0, description="Days since last activity")
    customer_lifetime_days: int = Field(ge=1, description="Customer lifetime in days")
    events_per_day: float = Field(ge=0, description="Events per day")
    purchase_frequency: float = Field(ge=0, description="Purchase frequency")
    events_last_30d: Optional[int] = Field(0, ge=0, description="Events in last 30 days")
    purchases_last_30d: Optional[int] = Field(0, ge=0, description="Purchases in last 30 days")
    page_views_last_30d: Optional[int] = Field(0, ge=0, description="Page views in last 30 days")
    active_days_last_30d: Optional[int] = Field(0, ge=0, description="Active days in last 30 days")
    avg_purchase_amount_last_30d: Optional[float] = Field(0, ge=0, description="Avg purchase amount in last 30 days")

class PredictionResponse(BaseModel):
    customer_id: str
    churn_probability: float
    risk_level: str
    prediction_timestamp: datetime
    model_version: str

class BatchPredictionRequest(BaseModel):
    customer_ids: List[str] = Field(..., description="List of customer IDs")

@app.on_event("startup")
async def startup_event():
    global predictor
    try:
        predictor = ChurnPredictor.load_model("/models/production/churn_model_latest.pkl")
        logger.info("Model loaded successfully")
    except Exception as e:
        logger.error("Failed to load model", error=str(e))
        predictor = None

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "model_loaded": predictor is not None
    }

@app.post("/predict", response_model=PredictionResponse)
async def predict_churn(features: CustomerFeatures):
    if predictor is None:
        raise HTTPException(status_code=503, detail="Model not available")

    try:
        # Convert to DataFrame
        feature_dict = features.dict()
        customer_id = feature_dict.pop('customer_id')
        df = pd.DataFrame([feature_dict])

        # Make prediction
        X, _ = predictor.prepare_features(df)
        churn_prob = predictor.predict_proba(X)[0]

        # Determine risk level
        if churn_prob >= 0.7:
            risk_level = "HIGH"
        elif churn_prob >= 0.4:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"

        response = PredictionResponse(
            customer_id=customer_id,
            churn_probability=float(churn_prob),
            risk_level=risk_level,
            prediction_timestamp=datetime.utcnow(),
            model_version="1.0"
        )

        # Store prediction asynchronously
        await store_prediction(response)

        return response

    except Exception as e:
        logger.error("Prediction failed", customer_id=features.customer_id, error=str(e))
        raise HTTPException(status_code=500, detail="Prediction failed")

@app.post("/predict/batch")
async def predict_batch(request: BatchPredictionRequest, background_tasks: BackgroundTasks):
    if predictor is None:
        raise HTTPException(status_code=503, detail="Model not available")

    try:
        # Get customer features from database
        features_df = sql_manager.get_customer_features(request.customer_ids)

        if features_df.empty:
            raise HTTPException(status_code=404, detail="No customer features found")

        # Make predictions
        X, _ = predictor.prepare_features(features_df)
        churn_probs = predictor.predict_proba(X)

        predictions = []
        for idx, row in features_df.iterrows():
            churn_prob = float(churn_probs[idx])

            if churn_prob >= 0.7:
                risk_level = "HIGH"
            elif churn_prob >= 0.4:
                risk_level = "MEDIUM"
            else:
                risk_level = "LOW"

            prediction = PredictionResponse(
                customer_id=row['customer_id'],
                churn_probability=churn_prob,
                risk_level=risk_level,
                prediction_timestamp=datetime.utcnow(),
                model_version="1.0"
            )
            predictions.append(prediction)

        # Store predictions in background
        background_tasks.add_task(store_batch_predictions, predictions)

        return {"predictions": predictions, "count": len(predictions)}

    except Exception as e:
        logger.error("Batch prediction failed", error=str(e))
        raise HTTPException(status_code=500, detail="Batch prediction failed")

@app.get("/customers/{customer_id}/prediction")
async def get_customer_prediction(customer_id: str):
    try:
        prediction = sql_manager.execute_query("""
            SELECT TOP 1
                customer_id,
                churn_probability,
                prediction_date,
                model_version
            FROM churn_predictions
            WHERE customer_id = :customer_id
            ORDER BY prediction_date DESC
        """, {'customer_id': customer_id})

        if prediction.empty:
            raise HTTPException(status_code=404, detail="No prediction found for customer")

        return prediction.to_dict(orient='records')[0]

    except Exception as e:
        logger.error("Failed to get customer prediction", customer_id=customer_id, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to retrieve prediction")

async def store_prediction(prediction: PredictionResponse):
    try:
        prediction_df = pd.DataFrame([{
            'customer_id': prediction.customer_id,
            'churn_probability': prediction.churn_probability,
            'prediction_date': prediction.prediction_timestamp,
            'model_version': prediction.model_version,
            'features': None
        }])

        sql_manager.insert_predictions(prediction_df)

    except Exception as e:
        logger.error("Failed to store prediction", error=str(e))

async def store_batch_predictions(predictions: List[PredictionResponse]):
    try:
        predictions_data = []
        for pred in predictions:
            predictions_data.append({
                'customer_id': pred.customer_id,
                'churn_probability': pred.churn_probability,
                'prediction_date': pred.prediction_timestamp,
                'model_version': pred.model_version,
                'features': None
            })

        predictions_df = pd.DataFrame(predictions_data)
        sql_manager.insert_predictions(predictions_df)

    except Exception as e:
        logger.error("Failed to store batch predictions", error=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
