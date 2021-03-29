# Databricks notebook source
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import concat, col, lit, to_timestamp, to_date, hour

print("Setup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC # Belgrade traffic accidents analysis
# MAGIC This notebook is used for data preparation (loading, transforming and storing). 

# COMMAND ----------

# MAGIC %md
# MAGIC ## First of, loading and transforming data about traffic accidents
# MAGIC - Loading **2015_2020.csv** file from filestore and renaming columns to something meaningful.<br>
# MAGIC - Using regular expression to replace *outcome* and *participant* values for easier analysis latter.<br>
# MAGIC - In the end, I casted *timestamp*, *latitude*, *longitude* columns and added *day_time* column for additional description. 

# COMMAND ----------

df_traffic = spark.read.option("header","true").csv("/FileStore/tables/2015_2020.csv").drop('_c0')
df_traffic = df_traffic.filter(df_traffic["1"]=='BEOGRAD')
df_traffic = df_traffic.select(col("0").alias("id"),col("1").alias("city"),col("2").alias("municipality")\
                               ,col("3").alias("timestamp"),col("4").alias("latitude"),col("5").alias("longitude")\
                              ,col("6").alias("outcome"),col("7").alias("participants"),col("8").alias("description"))
display(df_traffic)

# COMMAND ----------

replaces = [("Sa poginulim","death"),("Sa povredjenim","injury"),("Sa mat.stetom","damage")]
for exp,replace_exp in replaces:
  df_traffic = df_traffic.withColumn("outcome",regexp_replace(col("outcome"),exp,replace_exp).alias("outcome"))

display(df_traffic.select("outcome").distinct())


# COMMAND ----------

replaces = [("PARK","parked vehicle"),("NAJMANjE DVA","two or more vehicles"),("JEDNIM","one vehicle"),("ACIMA","pedestrian")]
for exp,replace_exp in replaces:
  exp = ".*("+exp+").*"
  df_traffic = df_traffic.withColumn("participants",regexp_replace(col("participants"),exp,replace_exp).alias("participants"))

display(df_traffic.select("participants").distinct())
df_traffic.show(5)

# COMMAND ----------

df_traffic = df_traffic.withColumn("timestamp", to_timestamp(df_traffic.timestamp, 'dd.MM.yyyy,HH:mm'))
df_traffic = df_traffic.withColumn("latitude", col("latitude").cast("double"))
df_traffic = df_traffic.withColumn("longitude", col("longitude").cast("double"))
df_traffic.printSchema()
df_traffic.show(5)

# COMMAND ----------

df_traffic.registerTempTable("accidents")
df_traffic = spark.sql("""select id, city, municipality, timestamp,
                  case
                    when hour(timestamp)>=4 and hour(timestamp)<7 then "early morning"
                    when hour(timestamp)>=7 and hour(timestamp)<11 then "morning"
                    when hour(timestamp)>=11 and hour(timestamp)<14 then "midday"
                    when hour(timestamp)>=14 and hour(timestamp)<18 then "afternoon"
                    when hour(timestamp)>=18 and hour(timestamp)<20 then "early evening"
                    when hour(timestamp)>=20 and hour(timestamp)<23 then "evening"
                    when hour(timestamp)>=23 or hour(timestamp)<1 then "midnight"
                    else "after midnight"
                  end as day_time,
                  latitude, longitude, outcome, participants, description
                  from accidents """)
df_traffic.registerTempTable("accidents")
df_traffic.show(10)
df_traffic.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Second, loading and transforming weather data
# MAGIC - Loading **sum.csv** and **obs.csv** files from filestore.<br>
# MAGIC - Casting *date* and *timestamp* columns, and all other columns that represent measurement data from string to double.<br>

# COMMAND ----------

df_summary = spark.read.format('csv').option('header','true').load('/FileStore/tables/sum.csv')
df_observations = spark.read.format('csv').option('header','true').load('/FileStore/tables/obs.csv')

df_summary.printSchema()
df_observations.printSchema()

# COMMAND ----------

df_summary = df_summary.withColumn('date', to_date(df_summary.date)).drop('_c0').distinct()
  
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
df_observations = df_observations.select(to_timestamp(col('timestamp')).alias('timestamp')\
                                                                 ,col('temperature'),col('humidity'),col('wind'),col('pressure'),col('condition'))

df_observations.show(5)
df_summary.show(5)

# COMMAND ----------

df = df_observations
df = df.select(df.timestamp, df.temperature.cast("double"),df.humidity.cast("double"),df.pressure.cast("double"),df.condition)
df.show(3)
df_observations = df

df = df_summary
df = df.select(df.date, df['high temp'].cast("double"),df['high temp historic'].cast("double"),\
               df['low temp'].cast("double"),df['low temp historic'].cast("double"),df['average'].cast("double"),\
               df['average historic'].cast("double"),df['max wind'].cast("double"),df['pressure'].cast("double"),df.sunrise,df.sunset)
df = df.select(df.date, df['high temp'].alias('high'), df['high temp historic'].alias('high_historic'), df['low temp'].alias('low'),\
              df['low temp historic'].alias('low_historic'), df.average, df['average historic'].alias('average_historic'),\
               df['max wind'].alias('max_wind'), df.pressure,df.sunrise,df.sunset)
df.show(3)
df_summary = df

# COMMAND ----------

df_summary.registerTempTable("summary")
df_observations.registerTempTable("observations")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Additional table
# MAGIC Joining tables *accidents* and *observations* makes it easier to query data about weather when accident occured.  

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table acc_obs;
# MAGIC create table acc_obs
# MAGIC select a.id, a.municipality, a.timestamp as accident, a.day_time, a.latitude, a.longitude, a.participants, a.outcome, w.timestamp as observation, w.condition, w.temperature, w.humidity, w.pressure
# MAGIC from accidents as a
# MAGIC inner join observations as w
# MAGIC on (date(a.timestamp)=date(w.timestamp) and (
# MAGIC (hour(a.timestamp)=hour(w.timestamp) and (
# MAGIC (hour(a.timestamp)=23 and minute(a.timestamp)>=30 and minute(w.timestamp)>=30) or
# MAGIC (minute(a.timestamp)<=15 and minute(w.timestamp)<=15) or 
# MAGIC (minute(a.timestamp)>15 and minute(a.timestamp)<45 and minute(w.timestamp)>15 and minute(w.timestamp)<45))) or
# MAGIC (minute(a.timestamp)>=45 and minute(w.timestamp)<15 and hour(w.timestamp)-hour(a.timestamp)=1)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storing data
# MAGIC Final step was storing data in MongoDB database.

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.mongodb.spark._
# MAGIC import spark.implicits._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val df_a = table("accidents")
# MAGIC val df_s = table("summary")
# MAGIC val df_o = table("observations")
# MAGIC val df_ao = table("acc_obs")
# MAGIC df_a.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").option("database", "belgrade_15-20").option("collection", "accidents").save()
# MAGIC df_s.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").option("database", "belgrade_15-20").option("collection", "summary").save()
# MAGIC df_o.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").option("database", "belgrade_15-20").option("collection", "observations").save()
# MAGIC df_ao.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").option("database", "belgrade_15-20").option("collection", "accidents_observations").save()
