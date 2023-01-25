import pyspark, pandas
from pyspark.sql import SparkSession

csv_file_path = "emplyoee.csv"
spark = SparkSession.builder.config("spark.jars", "/mnt/c/Users/Windows/Downloads/postgresql-42.5.1.jar") \
.master("local[*]").appName("pyspark_learning").getOrCreate()
df = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:32768/emp") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "accounts") \
    .option("user", "postgres").option("password", "postgrespw").load()
df.write.option("header", True).option("delimiter",",").csv(csv_file_path)



# spark = SparkSession.builder.config("spark.jars", "C:/Users/Windows/Downloads/postgresql-42.5.1.jar") \
# .master("local[*]").appName("pyspark_learning").getOrCreate()
# spark = SparkSession.builder.config("spark.jars", "/mnt/c/Users/Windows/Downloads/postgresql-42.5.1.jar") \
# .master("local[*]").appName("pyspark_learning").getOrCreate()
# df = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:32768/emp") \
#     .option("driver", "org.postgresql.Driver").option("dbtable", "accounts") \
#     .option("user", "postgres").option("password", "postgrespw").load()
# df.show()
# df.write.csv("/mnt/c/Users/Windows/Documents/WorkStore/employee_basics")
# df.write.option("header", True).option("delimiter",",").csv(csv_file_path)
# df.toPandas().to_csv("data.csv", header=True, index=False)


# /mnt/c/Users/Windows/Downloads/postgresql-42.5.1.jar

# /mnt/c/Users/Windows/Documents/WorkStore/basicDetails.csv

# df=spark.read.options(delimiter=",", header=True).csv("/mnt/c/Users/Windows/Documents/WorkStore/basicDetails.csv")






# import os
# os.environ["JAVA_HOME"] = 'C:\Program Files\Java\jdk-19'
# os.environ["SPARK_HOME"] = 'C:\Users\Windows\Documents\spark-3.3.1-bin-hadoop3'
# import findspark
# findspark.init()
# from pyspark.sql import SparkSession

# spark = SparkSession.builder.master("local[*]").getOrCreate()
#df=spark.read.options(delimiter=",", header=True).csv("C:/Users/Windows/Documents/WorkStore/basicDetails.csv")
# df=spark.read.options(delimiter=",", header=True).csv("/mnt/c/Users/Windows/Documents/WorkStore/basicDetails.csv")
# df.show()

# import findspark
# findspark.init("C:/Users/Windows/Documents/spark-3.3.1-bin-hadoop3")

#from pyspark import SparkContext
#sc = SparkContext(appName="EstimatePi")
#import random
#def inside(p):
#    x, y = random.random(), random.random()
#    return x*x + y*y < 1
#nUM_SAMPLES = 1000000
#count = sc.parallelize(range(0, nUM_SAMPLES)).filter(inside).count()
#print("Pi is roughly %f" % (4.0 * count / nUM_SAMPLES))
#sc.stop()

# spark = SparkSession.builder.appName('csql_demo1').master('local[*]').getOrCreate()
# print(spark)