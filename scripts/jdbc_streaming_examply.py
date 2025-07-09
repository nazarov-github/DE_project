import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_format, to_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурационные параметры
KAFKA_BROKER = "localhost:29092"
CLICKHOUSE_JDBC_URL = "jdbc:clickhouse://localhost:8123/default"
CLICKHOUSE_TABLE = "sensor_metrics"
CLICKHOUSE_USER = "admin"
CLICKHOUSE_PASSWORD = "clickhouse"

def create_spark_session():
    """Создание Spark сессии с поддержкой Kafka и ClickHouse JDBC"""
    try:
        spark = SparkSession.builder \
            .appName("KafkaToClickHouseJDBCStreaming") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                   "com.clickhouse:clickhouse-jdbc:0.4.6") \
            .getOrCreate()
        logger.info("Spark сессия успешно создана")
        return spark
    except Exception as e:
        logger.error(f"Ошибка при создании Spark сессии: {str(e)}")
        raise

def main():
    try:
        logger.info("Запуск streaming приложения с JDBC")
        
        # Создаем Spark сессию
        spark = create_spark_session()
        
        # Схема данных Kafka
        kafka_schema = StructType([
            StructField("id", StringType()),
            StructField("timestamp", StringType()),
            StructField("value", StructType([
                StructField("sensor_id", StringType()),
                StructField("temperature", DoubleType()),
                StructField("humidity", IntegerType()),
                StructField("pressure", IntegerType())
            ]))
        ])
        
        # Чтение данных из Kafka
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", "sensor_metrics") \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Парсинг JSON и преобразование структуры
        parsed_df = kafka_df \
            .select(
                from_json(col("value").cast("string"), kafka_schema).alias("data")) \
            .select(
                col("data.id").alias("message_id"),
                date_format(
                    to_timestamp(col("data.timestamp")),
                    "yyyy-MM-dd HH:mm:ss"
                ).alias("event_time"),
                col("data.value.sensor_id").alias("sensor_id"),
                col("data.value.temperature").alias("temperature"),
                col("data.value.humidity").alias("humidity"),
                col("data.value.pressure").alias("pressure")
            )
        
        # Запись в ClickHouse через JDBC
        def write_to_clickhouse_jdbc(batch_df, batch_id):
            try:
                batch_df.write \
                    .format("jdbc") \
                    .option("url", CLICKHOUSE_JDBC_URL) \
                    .option("dbtable", CLICKHOUSE_TABLE) \
                    .option("user", CLICKHOUSE_USER) \
                    .option("password", CLICKHOUSE_PASSWORD) \
                    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                    .mode("append") \
                    .save()
                
                logger.info(f"Успешно записано {batch_df.count()} строк в ClickHouse (batch {batch_id})")
            except Exception as e:
                logger.error(f"Ошибка записи в ClickHouse: {str(e)}")
                raise
        
        # Запуск streaming с JDBC writer
        query = parsed_df.writeStream \
            .foreachBatch(write_to_clickhouse_jdbc) \
            .outputMode("update") \
            .start()
        
        logger.info("Streaming приложение запущено")
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Ошибка в streaming приложении: {str(e)}")
        raise

if __name__ == "__main__":
    main()