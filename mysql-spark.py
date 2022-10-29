'''
spark-submit --packages mysql:mysql-connector-java:8.0.30 mysql-spark.py localhost sakila actor root Trellis#1 1 1 200 actor_id
'''

import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, count


# class with constructor
class mysql_spark:

	url = None	
	prop = None
	con1 = None
	query1 = None
	spark = None
	parms = {}
	
	# constructor 
	def __init__(self, host, db, tab, usr, pwd):
		
		print("inside constructor")
		self.spark = SparkSession.builder.appName("Read MySQL").getOrCreate()
		self.usr = usr
		self.pwd = pwd
		self.url = "jdbc:mysql://"+host+":3306"
		self.con1 = db+"."+tab
		self.prop = {"user":usr,"password":pwd}

	# decorator
	def gettime(func):
	
		def inner(*args):
			begin = time.time()
			res = func(*args)
			end = time.time()
			print("time taken", (end - begin))
			
			return res
			
		return inner


	# reads MySQL data into Spark dataframe
	@gettime
	def readMySQL(self,nP,lB,uB,pC):
		
		return self.spark.read \
				.format("jdbc") \
				.option("url",self.url) \
				.option("dbtable", self.con1) \
				.option("user",self.prop.get("user")) \
				.option("password", self.prop.get("password")) \
				.option("numPartitions", nP) \
				.option("lowerBound", lB) \
				.option("upperBound", uB) \
				.option("partitionColumn", pC) \
				.load()
	
if __name__ == "__main__":

	if len(sys.argv) != 10:
		print("Please check whether you are providing all the run time argument")
		exit(-1)

	host = sys.argv[1]
	db = sys.argv[2]
	tb = sys.argv[3]
	usr = sys.argv[4]
	pwd = sys.argv[5]
	nP = sys.argv[6]
	lB = sys.argv[7]
	uB = sys.argv[8]
	pC = sys.argv[9]
	
	obj = mysql_spark(host, db, tb, usr, pwd)	
	
	df = obj.readMySQL(nP,lB,uB,pC)
	
	df.show(truncate=False) 
	obj.spark.stop()  
