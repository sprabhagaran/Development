'''
This code will add a row_id column to a dataframe. 
'''

from pyspark.sql import SparkSession, Window
from pspark.sql.functions import Functions a F
from pyspark.sql.types import StructType, StringType, IntegerType


spark = Spark.builder.appName("row_id").getOrCreate()


data = [["Simran", 38, "Developer"],["Ashok", 40, "Auditor"],["Vimal", 35, "Doctor"])

schema = StructType().add("Name", StrngType()).add("Age", IntegerType()).add("Job", StringType())

df = spark.createDataFrame(data,schema)

df.show()

#adding row_id using zipWithIndex and rdd
df_with_id1 = df.rdd.zipWithIndex().toDF()

df_with_id1 = df_with_id1.select(df_with_id1._2.alias("row_num"),df_with_id1._1.Name.alias("Name"),df_with_id1._1.Age.alias("Age"))
df_with_id1.show()

#adding row_id using windows and row_number ordered by Age column
window = Window.orderBy(F.col("Age")

df_with_id2 = df.withColumn("row_num", F.row_number().over(window))
df_with_id2 = df_with_id2.select("row_num", df_with_id2.Name.alias("Name"), df_with_id2.Age.alias("Age"))

df_with_id2.show()

#adding row_id using windows and monotonically_increasing_id
window = Windows.orderBy(F.col('monotonically_increasing_id()'))

df_with_id3 = df.withColumn("row_id", F.row_number().over(window)
df_with_id3 = df_with_id3.select("row_id", df_with_id3.Name.alias("Name"), df_with_id3.Age.alias("Age"))

df_with_id3.show()

#adding row_id using sql function row_number and monotonically_increasing_id 
df.createOrReplaceTempView("tmp")
df_with_id4 = spark.sql("select row_number() over (monotonically_increasing_id()) as row_id, * from tmp)

df_with_id4.show()

spark.stop()
