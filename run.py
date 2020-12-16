#Analyzing Uber data with pyspark

#Connecting to Spark
import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName="myAppName")
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

#Importing the library to read the file
from pandas import read_csv

#Importing file
uberFile = read_csv("uber.csv")
print(type(uberFile))
#Printing the first 10 lines of the file
print(uberFile.head(10))


#Transform the imported file into an RDD
uberDF = sqlContext.createDataFrame(uberFile)
print(type(uberDF))

#Importing the direct file as RDD
uberRDD = sc.textFile("uber.csv")
print(type(uberRDD))
print(uberRDD.count())
print(uberRDD.first())


#Separate the file by lines
uberLinhas = uberRDD.map(lambda line: line.split(","))
print(type(uberLinhas))
print(uberLinhas.map(lambda linha: linha[0]).distinct().count())
print(uberLinhas.map(lambda linha: linha[0]).distinct().collect())
print(uberLinhas.filter(lambda linha: "B02617" in linha).count())

#Selecting a specific line as an example
b02617_RDD = uberLinhas.filter(lambda linha: "B02617" in linha)
print(b02617_RDD.filter(lambda linha: int(linha[3]) > 15000).count())
print(b02617_RDD.filter(lambda linha: int(linha[3]) > 15000).collect())

#Import the file as RDD and perform operations on the same line of code
uberRDD2 = sc.textFile("uber.csv").filter(lambda line: "base" not in line).map(lambda line:line.split(","))
print(uberRDD2.map(lambda kp: (kp[0], int(kp[3])) ).reduceByKey(lambda k,v: k + v).collect())
print(uberRDD2.map(lambda kp: (kp[0], int(kp[3])) ).reduceByKey(lambda k,v: k + v).takeOrdered(10, key = lambda x: -x[1]))
