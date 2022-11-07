'''
This code will add a row_id column to a dataframe. It is evident that converting a df to rdd and then appling zipwithindex is more efficient than other techiniques.
In other techniques all the partitions are copied into single partition and then the index is added, while in zipwithinde the index is added partition by partition.
'''

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


spark = SparkSession.builder.appName("row_id").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = [["Simran", 38, "Developer"],["Ashok", 40, "Auditor"],["Vimal", 35, "Doctor"]]

schema = StructType().add("Name", StringType()).add("Age", IntegerType()).add("Job", StringType())

df = spark.createDataFrame(data,schema)

print("******Original DataFrame******")
df.show()

#adding row_id using zipWithIndex starting at 0 and rdd
df_with_id1 = df.rdd.zipWithIndex().toDF()

df_with_id1 = df_with_id1.select(df_with_id1._2.alias("row_num"),df_with_id1._1.Name.alias("Name"),df_with_id1._1.Age.alias("Age"),df_with_id1._1.Job.alias("Job"))
print("******Zip with Index starts at 0******")
df_with_id1.show()

#adding row_id using zipWithIndex starting at 1 and rdd
df_with_id1 = df.rdd.zipWithIndex().map(lambda x : list(x[0]) + [x[1]+1]).toDF()
df_with_id1 = df_with_id1.select(df_with_id1._4.alias("row_num"),df_with_id1._1.alias("Name"),df_with_id1._2.alias("Age"),df_with_id1._3.alias("Job"))

print("******Zip with Index starts at 1******")
df_with_id1.show()


#adding row_id using windows and row_number ordered by Age column
window = Window.orderBy(F.col("Age"))

df_with_id2 = df.withColumn("row_num", F.row_number().over(window))
df_with_id2 = df_with_id2.select("row_num", df_with_id2.Name.alias("Name"), df_with_id2.Age.alias("Age"), df_with_id2.Job.alias("Job"))

print("******(windows + order by Age) starts at 1******")
df_with_id2.show()


#adding row_id using windows and monotonically_increasing_id
df = df.withColumn("row_num", F.monotonically_increasing_id())

window = Window.orderBy(F.col('row_num'))

df_with_id3 = df.withColumn("row_id", F.row_number().over(window))
df_with_id3 = df_with_id3.select("row_id", df_with_id3.Name.alias("Name"), df_with_id3.Age.alias("Age"), df_with_id3.Job.alias("Job"))

print("******(windows + monotonically_increasing_id) starts at 1******")
df_with_id3.show()

#adding row_id using sql function row_number and monotonically_increasing_id 
df.createOrReplaceTempView("tmp")
df_with_id4 = spark.sql("select row_number() over (order by 'monotonically_increasing_id()') as row_id, Name, Age, Job from tmp")

print("******(spark sql + monotonically_increasing_id) starts at 1******")
df_with_id4.show()

spark.stop()
