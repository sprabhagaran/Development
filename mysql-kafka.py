from kafka import KafkaProducer
import mysql.connector
from time import sleep
from json import dumps

#create a mysql connection
connection = mysql.connector.connect(host="localhost",database="development",user="****",password="******")

#create a curso to fetch all the records from mysql table employee
cursor = connection.cursor()
cursor.execute("select * from employee")
record = cursor.fetchall()
cursor.close()
connection.close()

#create an instance of th KafkaProducer connecting to local kafka server and JSON value serializer
producer = KafkaProducer(bootstrap_servers=["localhost:9092"],value_serializer= lambda m: dumps(m).encode('utf-8'))

#Loop through the records and set the data to be a json data and send the son data to kafka topic employee
for i in record:
	data={"id":i[0],"name":i[1],"age":i[2],"yoe":i[3],"salary":i[4]}
	producer.send("employee",data)
	sleep(1)
