from kafka import KafkaProducer
import mysql.connector
from time import sleep
from json import dumps

connection = mysql.connector.connect(host="localhost",database="development",user="root",password="Trellis#1")

cursor = connection.cursor()
cursor.execute("select * from employee")
record = cursor.fetchall()
cursor.close()
connection.close()

producer = KafkaProducer(bootstrap_servers=["localhost:9092"],value_serializer= lambda m: dumps(m).encode('utf-8'))

for i in record:
	data={"id":i[0],"name":i[1],"age":i[2],"yoe":i[3],"salary":i[4]}
	producer.send("employee",data)
	sleep(1)
