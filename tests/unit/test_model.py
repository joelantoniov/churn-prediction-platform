import pytest
import numpy as np
import pandas as pd
from src.model.churn_predictor import ChurnPredictor

@pytest.fixture
def sample_training_data():
    np.random.seed(42)
    n_samples = 1000
    
    data = {
        'total_events': np.random.randint(1, 100, n_samples),
        'unique_event_types': np.random.randint(1, 5, n_samples),
        'purchase_count': np.random.randint(0, 20, n_samples),
        'page_view_count': np.random.randint(0, 50, n_samples),
        'support_ticket_count': np.random.randint(0, 5, n_samples),
        'avg_purchase_amount': np.random.uniform(10, 500, n_samples),
        'total_purchase_amount': np.random.uniform(0, 1000, n_samples),
        'days_since_last_activity': np.random.randint(0, 30, n_samples),
        'customer_lifetime_days': np.random.randint(30, 365, n_samples),
        'events_per_day': np.random.uniform(0.1, 5, n_samples),
        'purchase_frequency': np.random.uniform(0, 1, n_samples),
        'events_last_30d': np.random.randint(0, 50, n_samples),
        'purchases_last_30d': np.random.randint(0, 10, n_samples),
        'page_views_last_30d': np.random.randint(0, 30, n_samples),
        'active_days_last_30d': np.random.randint(0, 30, n_samples),
        'avg_purchase_amount_last_30d': np.random.uniform(0, 300, n_samples)
    }
    
    df = pd.DataFrame(data)
    
    churn_probability = (
        (df['days_since_last_activity'] > 15).astype(int) * 0.3 +
        (df['purchase_count'] == 0).astype(int) * 0.4 +
        (df['events_last_30d'] < 5).astype(int) * 0.3
    )
    
    df['is_churned'] = (churn_probability > 0.5).astype(int)
    
    return df

def test_model_training(sample_training_data):
    predictor = ChurnPredictor()
    
    X, feature_names = predictor.prepare_features(sample_training_data)
    y = sample_training_data['is_churned'].values
    
    predictor.feature_names = feature_names
    metrics = predictor.train(X, y)
    
    assert predictor.is_trained
    assert 'accuracy' in metrics
    assert 'precision' in metrics
    assert 'recall' in metrics
    assert 'f1_score' in metrics
    assert 'auc_score' in metrics
    assert metrics['accuracy'] > 0.5

def test_model_prediction(sample_training_data):
    predictor = ChurnPredictor()
    
    X, feature_names = predictor.prepare_features(sample_training_data)
    y = sample_training_data['is_churned'].values
    
    predictor.feature_names = feature_names
    predictor.train(X, y)
    
    predictions = predictor.predict(X[:10])
    probabilities = predictor.predict_proba(X[:10])
    
    assert len(predictions) == 10
    assert len(probabilities) == 10
    assert all(p in [0, 1] for p in predictions)
    assert all(0 <= p <= 1 for p in probabilities)

def test_feature_importance(sample_training_data):
    predictor = ChurnPredictor()
    
    X, feature_names = predictor.prepare_features(sample_training_data)
    y = sample_training_data['is_churned'].values
    
    predictor.feature_names = feature_names
    predictor.train(X, y)
    
    importance = predictor.get_feature_importance()
    
    assert isinstance(importance, dict)
    assert len(importance) == len(feature_names)
    assert all(isinstance(v, (int, float)) for v in importance.values())
