# Документация проекта: Streaming Pipeline Kafka → Spark → ClickHouse

## Требования к окружению
- Python 3.8+
- Apache Spark 3.5.0
- ClickHouse 22.8+
- Kafka 2.8+
- Зависимости Python (см. requirements.txt):
  - pyspark==3.5.0
  - kafka-python==2.2.15
  - clickhouse-driver
  - requests==2.31.0

## Изменения в версии 1.1
- Исправлено подключение к localhost (KAFKA_BROKER и CLICKHOUSE_HOST)
- Добавлено логирование для отладки
- Улучшена обработка ошибок

## Веб-интерфейсы
- **Kafka UI**: доступен по адресу `http://localhost:8082`
  - Позволяет просматривать топики, сообщения, потребителей
  - Мониторинг состояния кластера Kafka
  - Управление топиками и ACL

## Архитектура решения
![Архитектура](https://i.imgur.com/xyz1234.png)

### Компоненты системы:
1. **Kafka** - брокер сообщений для потоковой передачи данных
2. **Spark Streaming** - обработка потоковых данных
3. **ClickHouse** - колоночная СУБД для аналитики

## Реализованная функциональность

### Основные функции:
- Прием данных сенсоров в реальном времени через Kafka
- Обработка данных с помощью Spark Streaming
- Сохранение в ClickHouse для аналитических запросов
- Генерация тестовых данных

### Параметры данных:
- Температура (Float64)
- Влажность (UInt32)
- Давление (UInt32)
- Метка времени (DateTime)
- ID сенсора (String)

## Проблемы и решения

### 1. Проблема аутентификации в ClickHouse
**Симптомы**: Ошибки подключения Spark к ClickHouse
**Решение**: Настройка правильных credentials в:
```yaml
# docker-compose.yml
environment:
  CLICKHOUSE_USER: admin
  CLICKHOUSE_PASSWORD: clickhouse
```

### 2. Проблема формата данных
**Симптомы**: Ошибки при вставке данных
**Решение**: Создание таблицы с правильными типами:
```sql
CREATE TABLE sensor_metrics (
    message_id String,
    event_time DateTime,
    sensor_id String,
    temperature Float64,
    humidity UInt32,
    pressure UInt32
) ENGINE = MergeTree()
ORDER BY (event_time, sensor_id)
```

### 3. Проблема запуска Kafka
**Симптомы**:
- Контейнер Kafka завершается с кодом 1
- В логах ошибка: `Error while creating ephemeral at /brokers/ids/1, node already exists`

**Решение**:
1. Проверить состояние Zookeeper:
```bash
docker exec de_project-zookeeper-1 zookeeper-shell localhost:2181 ls /brokers/ids
```

2. Если проблема сохраняется, полностью очистить окружение:
```bash
docker-compose down && docker volume rm $(docker volume ls -q | grep de_project) && docker-compose up -d
```

**Профилактика**:
- Всегда останавливайте Kafka/Zookeeper через `docker-compose down`
- Избегайте ручного удаления контейнеров без очистки томов

## Инструкции по запуску

### Запуск инфраструктуры:
```bash
docker-compose up -d zookeeper kafka clickhouse spark
```

### Генерация тестовых данных:
```bash
docker-compose exec spark python scripts/kafka_producer.py
```

### Запуск обработчика:
Вариант 1: Через docker-compose (рекомендуется)
```bash
docker-compose exec spark python scripts/streaming_example.py
```

Вариант 2: Напрямую (для отладки)
```bash
python3 scripts/streaming_example.py
```

### Проверка работы:
1. Проверить, что данные поступают в Kafka:
```bash
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic sensor_metrics \
  --from-beginning
```

2. Проверить запись в ClickHouse:
```bash
docker-compose exec clickhouse clickhouse-client \
  --user admin --password clickhouse \
  --query "SELECT count(), max(event_time) FROM sensor_metrics"
```

3. Проверить логи Spark:
```bash
docker-compose logs -f spark
```

## Мониторинг
- Kafka: `http://localhost:9000`
- ClickHouse: `http://localhost:8123`
- MinIO: `http://localhost:9001` (логин: minioadmin, пароль: minioadmin)

## Интеграция с MinIO

### Настройка клиента MinIO
```python
from minio import Minio

client = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)
```

### Примеры использования boto3 и S3Hook
1. Загрузка файла в MinIO:
```python
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

s3.upload_file('local_file.txt', 'data-lake', 'remote_file.txt')
```

2. Чтение файла из MinIO:
```python
response = s3.get_object(Bucket='data-lake', Key='remote_file.txt')
data = response['Body'].read()
```

3. Загрузка строки данных через S3Hook:
```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

hook = S3Hook(aws_conn_id="s3_conn")
hook.load_string(
    string_data=data,
    key="weather/weather_data.json",
    bucket_name="weather",
    replace=True
)
```

## Конфигурация volumes

В docker-compose.yml добавлены следующие volumes:
```yaml
volumes:
  minio_data:
    driver: local
  airflow_db:
    driver: local
  airflow_logs:
    driver: local
```

Назначение:
- `minio_data`: Хранилище для MinIO
- `airflow_db`: Данные PostgreSQL для Airflow
- `airflow_logs`: Логи Airflow