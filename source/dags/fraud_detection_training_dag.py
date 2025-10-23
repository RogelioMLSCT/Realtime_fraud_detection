from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
import logging
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)

default_args = {
    'owner': 'Rogelio Franco',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 20),
    'execution_timeout': timedelta(minutes=120),
    'max_active_runs': 1,    
}

def train_model(**context):
    """  Airflow wrapper for training task  """
    from fraud_detection_training import FraudDetectionTraining
    
    try:
        logger.info('Initializing fraud detection training')
        
        trainer = FraudDetectionTraining()
        
        
        return {'status': 'succes'}
    
    
    except Exception as e:
        logger.error(f'Training failed: {e} ')
        raise AirflowException(f'Model training failed')

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
        echo "Enviroment is valid!"
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
        - Transaction Data from Kafka
        - XGBOOST classifier with precision optimisation
        - MLFLOW for experimentation
    
    """