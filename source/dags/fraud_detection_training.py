import os
import logging
from dotenv import load_dotenv
import yaml
import mlflow
import boto3

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler('./fraud_detection_model.log'),
        logging.StreamHandler()
    ]
    
)

logger = logging.getLogger(__name__)


class FraudDetectionTraining:
    def __init__(self, config_path='/app/config.yaml'):
        os.environ['GIT_PYTHON_REFRESG'] = 'quiet'
        os.environ['GIT_PYTHON_GIT_EXECUTABLE'] = 'usr/bin/git'
        
        load_dotenv(dotenv_path='/app/.env')
         
        self.config = self._load_config(config_path)
        
        os.environ.update({
         'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID'),
         'AWS_SECRET_ACCESS_KEY':os.getenv('AWS_SECRET_ACCESS_KEY'),
         'AWS_S3_ENDPOINT_URL':self.config['mlflow']['s3_endpoint_url']
        })
        
        self._validate_enviroment()
        
        mlflow.set_tracking_uri(self.config['mlflow']['tracking_url'])
        mlflow.set_experiment(self.config['mlflow']['experiment_name'])
        
         
    def _load_config(self, config_path:str) -> dict:
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info('Configuration loaded succesfully')
            
            return config
        except Exception as e:
            logger.error(f'failed to load configuration: {e}')
            raise
    
    def _validate_enviromen(self):
        require_vars = ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_USERNAME', 'KAFKA_PASSWORD']
        missing = [var for var in require_vars if not os.getenv(var)]
        
        if missing:
            raise ValueError(f'Missing required eviroment variables: {missing}')
        
        self._check_minio_connection()
        
    def _check_minio_connection(self):
        try:
            s3 = boto3.client(
                's3',
                endpoint_url = self.config['mlflow']['s3_endpoint_url'],
                aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
                ) 
            
            buckets = s3.list_buckets()
            bucket_names =   [b['Name'] for b in buckets.get('Buckets', [])]
            logger.info(f'Minio connection verified. Buckets: %s', bucket_names)
            
            mlflow_bucket = self.config['mlflow'].get('bucket', 'mlflow')
            
            if mlflow_bucket not in bucket_names:
                s3.create_bucket(Bucket=mlflow_bucket)
                logger.info(f'Created missing Mlflow bucket: {mlflow_bucket}')
            
            
        
        except Exception as e:
            logger.error('Minio connection failed: {e}')
        
        
    
    