import joblib
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
import xgboost as xgb
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
from sklearn.preprocessing import StandardScaler
import mlflow
import mlflow.xgboost
import structlog

logger = structlog.get_logger()

class ChurnPredictor:
    def __init__(self, model_params: Optional[Dict[str, Any]] = None):
        self.model_params = model_params or {
            'max_depth': 6,
            'learning_rate': 0.1,
            'n_estimators': 100,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'random_state': 42,
            'eval_metric': 'auc'
        }
        self.model = None
        self.scaler = StandardScaler()
        self.feature_names = None
        self.is_trained = False
        
    def prepare_features(self, df: pd.DataFrame) -> Tuple[np.ndarray, List[str]]:
        feature_columns = [
            'total_events', 'unique_event_types', 'purchase_count', 
            'page_view_count', 'support_ticket_count', 'avg_purchase_amount',
            'total_purchase_amount', 'days_since_last_activity', 
            'customer_lifetime_days', 'events_per_day', 'purchase_frequency',
            'events_last_30d', 'purchases_last_30d', 'page_views_last_30d',
            'active_days_last_30d', 'avg_purchase_amount_last_30d'
        ]
        
        available_features = [col for col in feature_columns if col in df.columns]
        
        if not available_features:
            raise ValueError("No required features found in the dataset")
        
        feature_data = df[available_features].fillna(0)
        
        return feature_data.values, available_features
    
    def train(self, X: np.ndarray, y: np.ndarray, validation_split: float = 0.2) -> Dict[str, float]:
        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=validation_split, random_state=42, stratify=y
        )
        
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_val_scaled = self.scaler.transform(X_val)
        
        with mlflow.start_run():
            mlflow.log_params(self.model_params)
            
            self.model = xgb.XGBClassifier(**self.model_params)
            
            self.model.fit(
                X_train_scaled, y_train,
                eval_set=[(X_val_scaled, y_val)],
                early_stopping_rounds=10,
                verbose=False
            )
            
            y_pred = self.model.predict(X_val_scaled)
            y_pred_proba = self.model.predict_proba(X_val_scaled)[:, 1]
            
            metrics = {
                'accuracy': accuracy_score(y_val, y_pred),
                'precision': precision_score(y_val, y_pred),
                'recall': recall_score(y_val, y_pred),
                'f1_score': f1_score(y_val, y_pred),
                'auc_score': roc_auc_score(y_val, y_pred_proba)
            }
            
            mlflow.log_metrics(metrics)
            mlflow.xgboost.log_model(self.model, "churn_model")
            
            self.is_trained = True
            
            logger.info("Model training completed", metrics=metrics)
            
        return metrics
    
    def predict(self, X: np.ndarray) -> np.ndarray:
        if not self.is_trained:
            raise ValueError("Model must be trained before making predictions")
        
        X_scaled = self.scaler.transform(X)
        return self.model.predict(X_scaled)
    
    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if not self.is_trained:
            raise ValueError("Model must be trained before making predictions")
        
        X_scaled = self.scaler.transform(X)
        return self.model.predict_proba(X_scaled)[:, 1]
    
    def get_feature_importance(self) -> Dict[str, float]:
        if not self.is_trained:
            raise ValueError("Model must be trained to get feature importance")
        
        importance_scores = self.model.feature_importances_
        
        return dict(zip(self.feature_names, importance_scores))
    
    def save_model(self, filepath: str):
        if not self.is_trained:
            raise ValueError("Model must be trained before saving")
        
        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'feature_names': self.feature_names,
            'model_params': self.model_params
        }
        
        joblib.dump(model_data, filepath)
        logger.info("Model saved", filepath=filepath)
    
    @classmethod
    def load_model(cls, filepath: str):
        model_data = joblib.load(filepath)
        
        predictor = cls(model_data['model_params'])
        predictor.model = model_data['model']
        predictor.scaler = model_data['scaler']
        predictor.feature_names = model_data['feature_names']
        predictor.is_trained = True
        
        logger.info("Model loaded", filepath=filepath)
        
        return predictor
