import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_format, coalesce, when, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import requests
from requests.auth import HTTPBasicAuth

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурационные параметры
KAFKA_BROKER = "localhost:29092"
CLICKHOUSE_HOST = "localhost:8123"
CLICKHOUSE_DB = "default"
CLICKHOUSE_TABLE = "sensor_metrics"
CLICKHOUSE_USER = "admin"
CLICKHOUSE_PASSWORD = "clickhouse"

def create_spark_session():
    """Создание Spark сессии с поддержкой Kafka"""
    try:
        spark = SparkSession.builder \
            .appName("KafkaToClickHouseStreaming") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .getOrCreate()
        logger.info("Spark сессия успешно создана")
        return spark
    except Exception as e:
        logger.error(f"Ошибка при создании Spark сессии: {str(e)}")
        raise

def format_clickhouse_insert(data):
    """Формирование INSERT запроса для ClickHouse"""
    values = []
    for row in data:
        try:
            values.append(f"('{row['message_id']}', '{row['event_time']}', '{row['sensor_id']}', {row['temperature']}, {row['humidity']}, {row['pressure']})")
        except KeyError as e:
            logger.error(f"Отсутствует обязательное поле в данных: {e}")
            raise
    return f"INSERT INTO {CLICKHOUSE_TABLE} VALUES {','.join(values)}"

def write_to_clickhouse(batch_df, batch_id):
    """Функция для пакетной записи в ClickHouse через HTTP API"""
    try:
        count = batch_df.count()
        logger.info(f"Получен batch {batch_id} с {count} записями")
        
        if count == 0:
            logger.warning("Пустой batch - нет данных для записи")
            return
            
        # Конвертируем DataFrame в список словарей
        data = [row.asDict() for row in batch_df.collect()]
        logger.info(f"Первая запись в batch: {data[0]}")
        
        # Проверяем структуру данных
        sample_row = data[0]
        required_fields = ['message_id', 'event_time', 'sensor_id', 'temperature', 'humidity', 'pressure']
        for field in required_fields:
            if field not in sample_row:
                logger.error(f"Отсутствует обязательное поле {field} в данных")
                return
                
        logger.info("Данные прошли валидацию")
        
        # Формируем INSERT запрос
        query = format_clickhouse_insert(data)
        logger.info(f"Сформирован запрос длиной {len(query)} символов")
        
        # Отправляем запрос в ClickHouse
        url = f"http://{CLICKHOUSE_HOST}/?query={query}"
        logger.info(f"Отправка запроса в ClickHouse...")
        
        response = requests.post(
            url,
            auth=HTTPBasicAuth(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
            timeout=10
        )
        
        logger.info(f"Ответ от ClickHouse: статус {response.status_code}")
        if response.status_code != 200:
            logger.error(f"Ошибка записи в ClickHouse: {response.text}")
        else:
            logger.info(f"Успешно записано {len(data)} строк в ClickHouse")
            
    except Exception as e:
        logger.error(f"Критическая ошибка записи в ClickHouse: {str(e)}")
        raise

def main():
    try:
        logger.info("Запуск streaming приложения")
        
        # Создаем Spark сессию
        spark = create_spark_session()
        
        # Схема точно соответствующая предоставленному формату
        exact_schema = StructType([
            StructField("id", StringType()),
            StructField("timestamp", StringType()),
            StructField("value", StructType([
                StructField("sensor_id", StringType()),
                StructField("temperature", DoubleType()),
                StructField("humidity", IntegerType()),
                StructField("pressure", IntegerType())
            ]))
        ])
        
        logger.info("Подключение к Kafka...")
        # Читаем данные из Kafka
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", "sensor_metrics") \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", 1000) \
            .load() \
            .withColumn("offset", col("offset").cast("string")) \
            .withWatermark("timestamp", "1 minute")
        
        logger.info("Обработка данных из Kafka...")
        # Логируем оффсеты через консольный вывод
        def log_offsets(df):
            # Создаем временный streaming query для логирования
            df.writeStream \
              .outputMode("append") \
              .format("console") \
              .option("truncate", "false") \
              .start()
            
            return df

        # Детальный debug вывод сырых данных
        debug_raw = kafka_df.transform(log_offsets) \
            .select(
                col("offset"),
                col("timestamp"),
                col("value").cast("string").alias("raw_data")
            ) \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .start()

        # Логируем первые 5 сообщений через временный streaming query
        def log_sample_messages(df, epoch_id):
            sample = df.limit(5).collect()
            for i, row in enumerate(sample):
                logger.info(f"Sample message {i+1}:")
                logger.info(f"Offset: {row['offset']}")
                logger.info(f"Timestamp: {row['timestamp']}")
                logger.info(f"Value: {row['value'].decode('utf-8')}")
            return df
            
        kafka_df.writeStream \
            .foreachBatch(log_sample_messages) \
            .outputMode("append") \
            .start()

        # Парсим JSON с точной схемой
        parsed = kafka_df.transform(log_offsets) \
            .select(
                from_json(col("value").cast("string"), exact_schema).alias("data")
            )
        
        # Извлекаем данные из вложенной структуры
        parsed_df = parsed.select(
            col("data.id").alias("message_id"),
            date_format(
                to_timestamp(col("data.timestamp")),
                "yyyy-MM-dd HH:mm:ss"
            ).alias("event_time"),
            col("data.value.sensor_id").alias("sensor_id"),
            col("data.value.temperature").alias("temperature"),
            col("data.value.humidity").alias("humidity"),
            col("data.value.pressure").alias("pressure")
        ) \
            .filter(col("sensor_id").isNotNull()) \
            .filter(col("temperature").isNotNull()) \
            .withColumn("debug_info",
                when(col("message_id").isNotNull(), "valid_format")
                .otherwise("invalid_format"))
        
        logger.info("Запись данных в ClickHouse...")
        # Записываем данные в ClickHouse через foreachBatch
        query = parsed_df.writeStream \
            .outputMode("append") \
            .foreachBatch(write_to_clickhouse) \
            .start()
        
        # Добавляем debug вывод через консоль
        debug_query = parsed_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()
        
        logger.info("Streaming приложение запущено")
        query.awaitTermination()
        debug_query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Ошибка в streaming приложении: {str(e)}")
        raise

if __name__ == "__main__":
    main()