from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

df = spark.range(5)
df.show()

spark.stop()
print("✅ Spark работает!")
