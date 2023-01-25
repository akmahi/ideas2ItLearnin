import pyspark
from pyspark.sql import SparkSession

POSTGRES_JAR_PATH="/mnt/c/Users/Windows/Downloads/postgresql-42.5.1.jar"
URL="jdbc:postgresql://localhost:32768/emp"
TABLE="accounts"
USER="postgres"
PASSWORD="postgrespw"
FILE = "employee.csv"


spark = SparkSession.builder.config("spark.jars", POSTGRES_JAR_PATH).master("local[*]")\
        .appName("pyspark_learning").getOrCreate()
dataFrame = spark.read.format("jdbc").option("url",URL ) \
    .option("driver", "org.postgresql.Driver").option("dbtable",TABLE) \
    .option("user", USER).option("password", PASSWORD).load()
dataFrame.write.option("header", True).option("delimiter",",").csv(FILE)