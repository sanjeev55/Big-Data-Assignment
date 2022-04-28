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

datacube = spark.read.option("inferSchema","true").option("header","true").csv("2010-12-01.csv")
datacube.createOrReplaceTempView("dc")

# print(retailData.collect())

# print(datacube.show(5))

# Slice operation
slice = datacube.where(col("Country") == "United Kingdom")
# print(slice.show())

# Dice operation
dice = datacube.where(((col("Country") == "United States") | (col("Country") == "United Kingdom")) & (col("Quantity") == 0))
# print(dice.show())

# Roll up operation

roll_up = datacube.where(col("Quantity") > 0)
# print(roll_up.show())

# SQL Query 1 How many orders did customers perform at which hoour

spark.sql("SELECT StockCode, Country, SUM(Quantity) as total FROM dc GROUP BY StockCode,Country ORDER BY StockCode").show()

# SQL Query 2 How frequently was each product bought in different countries

spark.sql("SELECT count(Quantity),EXTRACT(HOURS from InvoiceDate) as time FROM dc group by time order by time").show()

# Spark Query 1 // How many orders did customers perform at which hoour

datacube.groupBy(hour('InvoiceDate')).count().orderBy(hour('InvoiceDate')).show()

# Spark Query 2 // How frequently was each product bought in different countries

datacube.groupBy('StockCode','Country').sum('Quantity').orderBy('StockCode').show()
# print(productBought.me())