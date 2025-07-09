# de_project

## Генератор тестовых данных для Kafka

Этот скрипт генерирует случайные данные датчиков (температура, влажность, давление) и отправляет их в Kafka.

### Требования
- Python 3.8+
- Установленные зависимости: `pip install -r requirements.txt`
- Запущенные сервисы Kafka, Zookeeper и MinIO (из docker-compose.yml)
- Дополнительные зависимости:
  - minio (для работы с S3-совместимым хранилищем)
  - boto3 (для работы с AWS S3)

### Установка
```bash
pip install -r requirements.txt
```

### Запуск
```bash
python scripts/kafka_producer.py
```

### Настройки Kafka
Сервис Kafka доступен:
- Внутри сети Docker: `kafka:9092`
- Снаружи (localhost): `localhost:29092`

Скрипт использует внешний порт `localhost:29092` и топик `sensor_metrics`.

### Пример данных
```json
{
  "sensor_id": "sensor_42",
  "temperature": 24.56,
  "humidity": 65,
  "pressure": 1012,
  "timestamp": "2025-07-04T23:50:30.123456"
}
```

### Остановка
Нажмите `Ctrl+C` для остановки генератора.

## Spark-Kafka-ClickHouse Streaming Pipeline

Приложение читает данные из Kafka, обрабатывает их и сохраняет в ClickHouse.

### Требования
- Spark 3.5.1 с пакетом spark-sql-kafka-0-10_2.12:3.5.1
- Kafka 3.4.1
- ClickHouse latest
- Docker и docker-compose

### Запуск
1. Запустите сервисы:
```bash
docker-compose up -d --build
```

2. Инициализируйте MinIO:
```bash
docker-compose exec minio mc alias set local http://minio:9000 minioadmin minioadmin
docker-compose exec minio mc mb local/data-lake
```

2. Создайте таблицу в ClickHouse:
```bash
docker-compose exec clickhouse clickhouse-client --user admin --password clickhouse --query "CREATE TABLE IF NOT EXISTS sensor_metrics (
    message_id String,
    event_time DateTime,
    sensor_id String,
    temperature Float64,
    humidity UInt32,
    pressure UInt32
) ENGINE = MergeTree()
ORDER BY (event_time, sensor_id)"
```

3. Запустите streaming приложение:
```bash
docker-compose exec spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 scripts/streaming_example.py
```

4. Проверьте данные в ClickHouse:
```bash
docker-compose exec clickhouse clickhouse-client --user admin --password clickhouse --query "SELECT count() FROM sensor_metrics"
```

### Мониторинг
- Spark UI доступен на http://localhost:4040
- Kafka consumer для проверки данных:
```bash
docker-compose exec kafka kafka-console-consumer --topic sensor_metrics --from-beginning --bootstrap-server localhost:29092
```

### Остановка
1. Остановите streaming приложение (Ctrl+C)
2. Остановите сервисы:
```bash
docker-compose down -v
```

## Устранение проблем

### Проблемы с Airflow
**Симптомы**: Ошибки подключения к базе данных или веб-интерфейсу

**Решение**:
1. Проверить состояние контейнеров:
```bash
docker-compose ps
```

2. Пересоздать контейнеры:
```bash
docker-compose down -v && docker-compose up -d
```

3. Проверить логи:
```bash
docker-compose logs -f airflow-webserver
```

**Профилактика**:
- Убедитесь, что порты 8080 и 5432 свободны
- Проверьте настройки подключения к базе данных в docker-compose.yml