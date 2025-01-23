from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark Shell Example") \
    .config("spark.executor.instances", "4")
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Verify settings
print(spark.sparkContext.getConf().getAll())

