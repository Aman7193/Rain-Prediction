from pyspark import SparkConf
from pyspark import SparkContext

appName = "demo1"
master = "local"
cnf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=cnf)

file  =  sc.textFile("/home/amandeep/spark-3.3.1-bin-hadoop3/RELEASE")

result = file\
    .map(lambda line : line.lower())\
    .flatMap(lambda line : line.split())\
    .map(lambda word : (word,1))\
    .reduceByKey(lambda a,i:a+i)\
    .map(lambda wordcnt : (wordcnt[0].upper(),wordcnt[1]))\
    .collect()

print(result)

sc.stop()