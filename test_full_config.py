# test_full_config.py
from pyspark import SparkConf
from pyspark.sql import SparkSession
from config import SparkConfig

# Тестируем полную конфигурацию
config = SparkConfig(
    app_name="FullConfigTest",
    master="local[*]",
    driver_memory="1g",
    executor_memory="1g",
    shuffle_partitions=50,
    adaptive_enabled=True,
)

conf = config.create_spark_conf()
spark = SparkSession.builder.config(conf=conf).getOrCreate()

print("✅ Полная конфигурация работает!")
print(f"App: {spark.sparkContext.appName}")
print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

spark.stop()
