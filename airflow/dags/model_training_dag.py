from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-team@company.com']
}

dag = DAG(
    'churn_model_training',
    default_args=default_args,
    description='Daily churn model training and deployment',
    schedule_interval='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'churn', 'training']
)

def extract_training_data():
    from src.storage.delta_lake_manager import DeltaLakeManager
    from config.spark_config import SparkConfig
    
    spark = SparkConfig.create_spark_session()
    delta_manager = DeltaLakeManager(spark)
    
    features_df = delta_manager.read_table("/data/customer_features")
    labels_df = delta_manager.read_table("/data/churn_labels")
    
    training_data = features_df.join(labels_df, "customer_id")
    training_data.write.mode("overwrite").parquet("/tmp/training_data")
    
    spark.stop()

def train_model():
    import pandas as pd
    from src.model.churn_predictor import ChurnPredictor
    
    training_df = pd.read_parquet("/tmp/training_data")
    
    predictor = ChurnPredictor()
    X, feature_names = predictor.prepare_features(training_df)
    y = training_df['is_churned'].values
    
    predictor.feature_names = feature_names
    metrics = predictor.train(X, y)
    
    predictor.save_model("/models/churn_model_latest.pkl")
    
    return metrics

def deploy_model():
    import shutil
    from datetime import datetime
    
    model_version = datetime.now().strftime("%Y%m%d_%H%M")
    
    shutil.copy("/models/churn_model_latest.pkl", f"/models/production/churn_model_{model_version}.pkl")
    
    with open("/models/production/current_model.txt", "w") as f:
        f.write(f"churn_model_{model_version}.pkl")

extract_data_task = PythonOperator(
    task_id='extract_training_data',
    python_callable=extract_training_data,
    dag=dag
)

train_model_task = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    dag=dag
)

deploy_model_task = PythonOperator(
    task_id='deploy_model',
    python_callable=deploy_model,
    dag=dag
)

feature_engineering_task = SparkSubmitOperator(
    task_id='feature_engineering',
    application='/opt/airflow/jobs/feature_engineering.py',
    conn_id='spark_default',
    dag=dag
)

send_notification = EmailOperator(
    task_id='send_success_notification',
    to=['data-team@company.com'],
    subject='Churn Model Training Completed',
    html_content='<p>Daily churn model training completed successfully.</p>',
    dag=dag
)

extract_data_task >> feature_engineering_task >> train_model_task >> deploy_model_task >> send_notification
