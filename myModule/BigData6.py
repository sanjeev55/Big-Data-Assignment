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

df = spark.read.option("inferSchema","true").option("header","true").csv("2015-summary.csv")

# df.groupBy("Dest").sum("miles").withColumnRenamed("sum(miles)", "destination_total").sort("destination_total").limit(5).explain()

# df.where(col("DEST_COUNTRY_NAME") == "United States").groupBy("ORIGIN_COUNTRY_NAME").agg(sum("count")).sort("ORIGIN_COUNTRY_NAME").explain()

df.filter(df.DEST_COUNTRY_NAME == "United States").groupBy("ORIGIN_COUNTRY_NAME").sum("count").sort("ORIGIN_COUNTRY_NAME").explain()

# == Physical Plan ==
# *(3) Sort [ORIGIN_COUNTRY_NAME#17 ASC NULLS FIRST], true, 0
#            +- Exchange rangepartitioning(ORIGIN_COUNTRY_NAME#17 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [id=#51]
#                  +- *(2) HashAggregate(keys=[ORIGIN_COUNTRY_NAME#17], functions=[sum(cast(count#18 as bigint))])
#                          +- Exchange hashpartitioning(ORIGIN_COUNTRY_NAME#17, 200), ENSURE_REQUIREMENTS, [id=#47]
#                                                 +- *(1) HashAggregate(keys=[ORIGIN_COUNTRY_NAME#17], functions=[partial_sum(cast(count#18 as bigint))])
#                                                                          +- *(1) Project [ORIGIN_COUNTRY_NAME#17, count#18]
#                                                                                             +- *(1) Filter (isnotnull(DEST_COUNTRY_NAME#16) AND (DEST_COUNTRY_NAME#16 = United States))
#                                                                                                                                              +- FileScan csv [DEST_COUNTRY_NAME#16,ORIGIN_COUNTRY_NAME#17,count#18] Batched: false, DataFilters: [isnotnull(DEST_COUNTRY_NAME#16), (DEST_COUNTRY_NAME#16 = United States)], Format: CSV, Location: