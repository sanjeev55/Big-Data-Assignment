import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Above code is the solution for "cannot find python3".
# Package os allows you to set global variables; package sys gives the string with the absolute path of the executable binary for the Python interpreter.

import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import pyspark



sc = pyspark.SparkContext("local", "Assignment 10")
sc.setLogLevel("ERROR")
spark = pyspark.sql.SparkSession(sc)

order_items = [("red", "sour", "yes"),
               ("red", "sour", "no"),
               ("red", "bitter", "no"),
               ("yellow", "sweet", "yes"),
               ("yellow", "bitter", "no"),
               ("green", "sour", "yes"),
               ("green", "sweet", "yes"),
               ("green", "bitter", "no"),
               ]

schema = StructType([
    StructField("PulpColor", StringType(), True),
    StructField("Taste", StringType(), True),
    StructField("Edible", StringType(), True),
])

df = spark.createDataFrame(data=order_items, schema=schema)
df.show()

datasize = df.count()
yesDf = df.filter(f.expr("Edible = \"yes\""))
yesDf.show()

yes = yesDf.count()
yesYellow = yesDf.filter(f.expr("PulpColor = \"yellow\"")).count()
yesSour = yesDf.filter(f.expr("Taste = \"sour\"")).count()
likelihoodYes = (yes / datasize) * yesYellow / yes * yesSour / yes

noDf = df.filter(f.expr("Edible = \"no\""))
no = noDf.count()
noYellow = noDf.filter(f.expr("PulpColor = \"yellow\"")).count()
noSour = noDf.filter(f.expr("Taste = \"sour\"")).count()
likelihoodNo = (no / datasize) * noYellow / no * noSour / no

if likelihoodYes > likelihoodNo:
    print("yes")
else:
    print("no")

