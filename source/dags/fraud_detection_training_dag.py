from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)

default_args = {
    'owner': 'datamasterylab.com',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 3),
    ##'execution_timeout': timedelta(minutes=120),
    'max_active_runs': 1,    
}

def train_model(**context):
    """  Airflow wrapper for training task  """
    # from fraud_detection_training import FraudDetectionTraining
    pass

with DAG( 'Fraud_detection_training',
    default_args=default_args,
    description='Fraud detecion model training pipeline',
    schedule_interval='0 3  * * *',
    catchup=False,
    tags=['fraud', 'ML']
) as dag:
    validate_enviroment = BashOperator(
        task_id = 'validate_enviroment',
        bash_command= '''
        echo "Validating enviroment......"
        test -f /app/config.yaml &&
        test -f /app/.env &&
        echo "Enviroment is valid!
        '''       
    )
    
    training_task = PythonOperator(
        task_id='execute_training',
        python_callable=train_model,
        provide_context=True
        )
    
    
    cleanup_task = BashOperator(
        task_id= 'cleanup_resources',
        bash_command= 'rm -f /app/tmp/*.pkl',
        trigger_rule='all_done'
    )
    
    # order of executions
    validate_enviroment >> training_task >> cleanup_task
    
    
    # Documentacion
    dag.doc_md = """
    ## Fraud Detection Training Pipeline
    
    Daily training of fraud detection model using:
        - Training model using XGBOOST
    
    """