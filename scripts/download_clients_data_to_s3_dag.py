from airflow.decorators import dag, task
from datetime import datetime
from download_clients_data_to_s3 import download_clients_data_to_s3
import logging

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025,7, 12),
    "retries": 1
}

@dag(
    schedule_interval='*/10 * * * *',
    catchup=False,
    default_args=default_args
)

def download_client_data_to_s3_dag():
    @task()
    def download_clients():
        try:
            download_clients_data_to_s3()
        except Exception as e:
            logger.error(f'Error: {e}')
            raise



    download_clients_task = download_clients()



download_client_data_to_s3_dag = download_client_data_to_s3_dag()

