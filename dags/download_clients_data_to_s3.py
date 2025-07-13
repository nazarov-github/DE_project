import logging
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import mimesis
from mimesis import Person, Generic, Address, Finance, Datetime, Choice
from mimesis.locales import Locale
from mimesis import Code
from mimesis.enums import TimestampFormat
from datetime import datetime


# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

person = Person(Locale.EN)
generic = Generic()
code = Code()
address = Address()
finance = Finance()
datetime_gen = Datetime()
choice = Choice()

def download_clients_data_to_s3():
    lst_data = []
    logger.info("Генерация тестовых данных началась")
    try:
        for _ in range(101):
            data = {
                "personal_info": {
                    "name": person.first_name(),
                    "last_name": person.last_name(),
                    "username": person.username(),
                    "birthday": person.birthdate().isoformat(),
                    "email": person.email(),
                    "telephone_number": person.telephone(),
                    "is_active": person.random.choice([True, False]),
                    "verified": person.random.choice([True, False])
                },
                "address": {
                    "country": address.country(),
                    "city": address.city(),
                    "street": address.street_name(),
                    "postal_code": address.postal_code()
                },
                "company": {
                    "name": finance.company(),
                    "type": finance.company_type(),
                },
                "finance": {
                    "bank": finance.bank(),
                    "bank_account": code.imei()
                },
                "metadata": {
                    "registration_date": datetime_gen.timestamp(fmt=TimestampFormat.RFC_3339)
                }
            }
    
                
            lst_data.append(data)
    except Exception as e:
        logger.error(f"Данные не сгенерированы по причине ошибки {e}")
        raise
    
    

    json_data = json.dumps(lst_data, indent=4)
    logger.info("Данные успешно сгенерированы")

    file_name = f"clients_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    # Проверка подключения к S3
    try:
        hook = S3Hook(aws_conn_id="s3_conn")
        hook.check_for_bucket(bucket_name="clientsdata")
    except Exception as e:
        logger.error(f"Ошибка подключения к S3: {e}")
        raise
        
    if json_data:
        try:
            hook.load_string(
                string_data=json_data,
                key=f"clientsdata/{file_name}",
                bucket_name="clientsdata",
                replace=True
            )
            logger.info(f"Данные успешно записаны в S3")
        except Exception as e:
            logger.error(f"Ошибка {e}")
            raise
    else:
        logger.error(f"Данных нет")




