import findspark
findspark.init()
import pyspark
import pandas as pd
from pyspark.sql.functions import *

spark = (pyspark
         .sql
         .SparkSession
         .builder
         .master("local")
         .getOrCreate())

retail_data = spark.read.option("inferSchema","true").option("header","true").csv("online-retail-dataset.csv")
# retail_data.show()


retail_data.groupBy("StockCode").pivot("Country").sum("Quantity").show()
