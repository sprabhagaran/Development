from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import col, from_json

spark = SparkSession.builder.appName("kafka-spark").getOrCreate()

df = spark.readStream\
	.format("kafka")\
	.option("kafka.bootstrap.servers","localhost:9092")\
	.option("subscribe","employee")\
	.option("startingOffsets","earliest")\
	.load()

df1 = df.selectExpr("cast(value as string)")

json_schema = StructType().add("id",IntegerType()).add("name",StringType()).add("age",IntegerType()).add("salary",IntegerType())
df2 = df1.select(from_json(col("value"),json_schema))

df2.writeStream\
	.format("console")\
	.outputMode("append")\
	.start()\
	.awaitTermination()
