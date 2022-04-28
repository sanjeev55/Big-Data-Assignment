import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import pyspark
import pandas as pd

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark = SparkSession.builder.appName('Big Data Assignment one').getOrCreate()
sc = spark.sparkContext


dataframe = pd.read_csv('income_evaluation.csv')

print(df.info())

# countCol = df['count']
# maxValue = countCol.max()
# print(maxValue)
#
# flightCount = df[df['count'] > 10]
#
# print(flightCount.count())
#
# destCount = df[df["DEST_COUNTRY_NAME"] == "United States"]
#
# totalFlight = destCount["count"]
#
#
#
# print(totalFlight.sum())
#
#
# words = sc.parallelize (
#     ["Big Data",
#      "Data Science",
#      "Intro to Web",
#      "Web Engineering",
#      "Network Theory",
#      "Machine Learning",
#      ]
# )
# words_map = words.map(lambda x: (x, "a"))
# print(words_map.collect())
#
#
# words = sc.parallelize (
#     ["Big Data", "Data Science", "Intro to Web",  "Web Engineering",  "Network Theory", "Machine Learning", ]
# )
# words_map = words.filter(lambda x: "Data" in x)
# print(words_map.collect())