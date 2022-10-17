import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":

	if len(sys.argv) != 6:
		print("Please check whether you are providing all the run time argument")
		exit(-1)

	host = sys.argv[1]
	db = sys.argv[2]
	tab = sys.argv[3]
	usr = sys.argv[4]
	pwd = sys.argv[5] 
	
	url = "jdbc:mysql://"+host+":3306"
	con = db+"."+tab
	prop = {"user":usr,"password":pwd}

	spark = SparkSession.builder.appName("Read MySQL").getOrCreate()

	df = spark.read.jdbc(url,con,properties=prop)

	df.show()
	
	spark.stop()  
