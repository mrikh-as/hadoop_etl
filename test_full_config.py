from dotenv import load_dotenv
from pyspark import SparkConf
from pyspark.sql import SparkSession
from spark_config import SparkConfig

load_dotenv()
config = SparkConfig.from_env()
conf = config.create_spark_conf()
spark = SparkSession.builder.config(conf=conf).getOrCreate()

print("✅ Полная конфигурация работает!")
print(f"App: {spark.sparkContext.appName}")
print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

spark.stop()
