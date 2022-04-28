import findspark
findspark.init()
import pyspark
import pandas as pd
import numpy as np
from pyspark.sql.functions import col
from pyspark.sql.functions import mean
from pyspark.sql.functions import stddev_pop
from pyspark.sql.functions import max

spark = (pyspark
         .sql
         .SparkSession
         .builder
         .master("local")
         .getOrCreate())

# create dataframe
dataframe = pd.DataFrame({'school_year':[1,2,3,4], 'busbreakdownID':[12,13,12,13],'runType':['video','audio','videonew','audionew'],
                          'busType':['youtube','yt','insta','fb'],
                   'routeNumber':[2,4,5,6],'schoolServiced':[3,5,7,8],
                     'occuredOn':[2016,2018,2019,2017],'town':['Kat','Pok','Chit','Ger']})
busbreakdownData = spark.createDataFrame(dataframe)

result = result = busbreakdownData.na.drop("all", subset=["occuredOn"])
print(result.show())
busbreakdownData.filter(col("town") == 'Kat').groupby('occuredOn').agg('busbreakdownID')





# spark.sql("SELECT town, school_year(busbreakdownData) AS year, count(busbreakdownData) AS totalBusBreakDownData FROM busbreakdownData GROUP BY town, year WITH CUBE ORDER BY , year, town, routeNumber")





# print(dataframe.show())
#
# # selection question
# videoStats = dataframe.select(col("video_id"),col("trending_date"),col("title"),col("views"))
#
# print(videoStats.show())
#
# #add new column in the dataframe
# videoStats = videoStats.withColumn("new",dataframe["views"]/100)
# print(videoStats.show())
#
# #find mean, standard deviation of the population and maximum views
#
# meanValue = videoStats.select(mean(col("views")))
#
# print(meanValue.show())
#
# standardDev = videoStats.select(stddev_pop(col("views")))
#
# print(standardDev.show())
#
# maxValue = videoStats.groupBy().max("views").collect()[0]
#
# print(maxValue)
#
# # create new dataFrame using groupBy and mean
#
# videoStatGroup = videoStats.groupBy("trending_date").mean("views")
#
# print(videoStatGroup.show())
