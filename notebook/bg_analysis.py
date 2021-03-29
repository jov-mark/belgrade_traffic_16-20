# Databricks notebook source
# MAGIC %md
# MAGIC # Belgrade traffic accidents analysis
# MAGIC This notebook is used for analysis of available data. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading data
# MAGIC Firstly, loading data from MongoDB database

# COMMAND ----------

# MAGIC %scala
# MAGIC import spark.implicits._
# MAGIC import com.mongodb.spark._
# MAGIC 
# MAGIC val dfa = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "belgrade_15-20").option("collection", "accidents").load()
# MAGIC val dfs = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "belgrade_15-20").option("collection", "summary").load()
# MAGIC val dfo = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "belgrade_15-20").option("collection", "observations").load()
# MAGIC val dfao = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "belgrade_15-20").option("collection", "accidents_observations").load()
# MAGIC 
# MAGIC dfa.createOrReplaceTempView("accidents")
# MAGIC dfs.createOrReplaceTempView("summary")
# MAGIC dfo.createOrReplaceTempView("observations")
# MAGIC dfao.createOrReplaceTempView("accidents_observations")

# COMMAND ----------

dfa = spark.sql("select id, city, municipality, timestamp, day_time, latitude, longitude, outcome, participants, description from accidents")
dfs = spark.sql("select date(date), ROUND(((high - 32) * 5/9)) as high, ROUND(((high_historic - 32) * 5/9)) as high_historic, ROUND(((low - 32) * 5/9)) as low,ROUND(((low_historic - 32) * 5/9)) as low_historic, ROUND(((average - 32) * 5/9)) as average,ROUND(((average_historic - 32) * 5/9)) as average_historic,max_wind, pressure, sunrise, sunset from summary")
dfo = spark.sql("select timestamp, ROUND(((temperature - 32) * 5/9)) as temperature, humidity, pressure, condition from observations")
dfao = spark.sql("select id, municipality, accident, day_time, latitude, longitude, outcome, participants, observation, condition, ROUND(((temperature - 32) * 5/9)) as temperature, humidity, pressure from accidents_observations")
dfa.createOrReplaceTempView("accidents")
dfs.createOrReplaceTempView("summary")
dfo.createOrReplaceTempView("observations")
dfao.createOrReplaceTempView("acc_obs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining questions
# MAGIC - When do most accidents occur?
# MAGIC - Does temperature or weather condition affect frequency of traffic accidents?
# MAGIC - What is the most dangerous municipality for driving?
# MAGIC - What is the most dangerous municipality for pedestrians? 

# COMMAND ----------

# MAGIC %sql
# MAGIC select municipality, count(id) as number from accidents
# MAGIC where date(timestamp) between '2016-01-01' and '2020-12-31'
# MAGIC group by municipality
# MAGIC order by number desc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table mun;
# MAGIC create table mun
# MAGIC select * from accidents
# MAGIC where year(timestamp)>=2016 and (municipality="NOVI BEOGRAD" or municipality="VOŽDOVAC" or municipality="PALILULA")

# COMMAND ----------

# MAGIC %md
# MAGIC For simplicity sake, I decided to fokus on top three municipalities with highest number of accidents. Those are *Novi Beograd*, *Voždovac* and *Palilula*.

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table accidents_by_municipality;
# MAGIC create table accidents_by_municipality
# MAGIC select municipality, year(timestamp) as year, month(timestamp) as month, count(id) as number from accidents
# MAGIC where date(timestamp) between '2016-01-01' and '2020-12-31'
# MAGIC group by municipality, year, month
# MAGIC order by year, month

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   municipality, 
# MAGIC   sum(number) as sum,
# MAGIC   SUM(CASE WHEN month=1 THEN number END) AS `1`,
# MAGIC   SUM(CASE WHEN month=2 THEN number END) AS `2`,
# MAGIC   SUM(CASE WHEN month=3 THEN number END) AS `3`,
# MAGIC   SUM(CASE WHEN month=4 THEN number END) AS `4`,
# MAGIC   SUM(CASE WHEN month=5 THEN number END) AS `5`,
# MAGIC   SUM(CASE WHEN month=6 THEN number END) AS `6`,
# MAGIC   SUM(CASE WHEN month=7 THEN number END) AS `7`,
# MAGIC   SUM(CASE WHEN month=8 THEN number END) AS `8`,
# MAGIC   SUM(CASE WHEN month=9 THEN number END) AS `9`,
# MAGIC   SUM(CASE WHEN month=10 THEN number END) AS `10`,
# MAGIC   SUM(CASE WHEN month=11 THEN number END) AS `11`,
# MAGIC   SUM(CASE WHEN month=12 THEN number END) AS `12`
# MAGIC FROM accidents_by_municipality
# MAGIC GROUP BY municipality
# MAGIC ORDER BY sum DESC
# MAGIC LIMIT 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   municipality, 
# MAGIC   year,
# MAGIC   SUM(CASE WHEN month=1 THEN number END) AS `1`,
# MAGIC   SUM(CASE WHEN month=2 THEN number END) AS `2`,
# MAGIC   SUM(CASE WHEN month=3 THEN number END) AS `3`,
# MAGIC   SUM(CASE WHEN month=4 THEN number END) AS `4`,
# MAGIC   SUM(CASE WHEN month=5 THEN number END) AS `5`,
# MAGIC   SUM(CASE WHEN month=6 THEN number END) AS `6`,
# MAGIC   SUM(CASE WHEN month=7 THEN number END) AS `7`,
# MAGIC   SUM(CASE WHEN month=8 THEN number END) AS `8`,
# MAGIC   SUM(CASE WHEN month=9 THEN number END) AS `9`,
# MAGIC   SUM(CASE WHEN month=10 THEN number END) AS `10`,
# MAGIC   SUM(CASE WHEN month=11 THEN number END) AS `11`,
# MAGIC   SUM(CASE WHEN month=12 THEN number END) AS `12`
# MAGIC FROM accidents_by_municipality
# MAGIC WHERE municipality="NOVI BEOGRAD"
# MAGIC GROUP BY municipality, year
# MAGIC ORDER BY year

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table accidents_in_lockdown;
# MAGIC create table accidents_in_lockdown
# MAGIC select municipality, 
# MAGIC        day,
# MAGIC        year(timestamp) as year,
# MAGIC        count(id) as number
# MAGIC from (select id,
# MAGIC        municipality,
# MAGIC        timestamp,
# MAGIC        day_time,
# MAGIC        weekday(timestamp) as day,
# MAGIC        outcome,
# MAGIC        participants
# MAGIC        from accidents)
# MAGIC where (date(timestamp) between '2019-04-01' and '2019-04-30' or date(timestamp) between '2020-04-01' and '2020-04-30')
# MAGIC group by municipality, day, year
# MAGIC order by municipality

# COMMAND ----------

# MAGIC %sql
# MAGIC select municipality,
# MAGIC        NVL(sum(case when year=2019 and day>=5 then number end),0) as `weekend2019`,
# MAGIC        NVL(sum(case when year=2020 and day>=5  then number end),0) as `weekend2020`,
# MAGIC        NVL(sum(case when year=2019 and day<=4  then number end),0) as `workdays2019`,
# MAGIC        NVL(sum(case when year=2020 and day<=4 then number end),0) as `workdays2020`,
# MAGIC        sum(case when year=2019 then number end) as `2019`,
# MAGIC        sum(case when year=2020 then number end) as `2020`
# MAGIC from accidents_in_lockdown
# MAGIC where municipality="NOVI BEOGRAD" or municipality="PALILULA" or municipality="VOŽDOVAC"
# MAGIC group by municipality

# COMMAND ----------

# MAGIC %md
# MAGIC There's inconsistency in the number of accidents between 2020. and previous years. Bearing in mind that 2020 is the year of corona virus pandemic and that Serbian government periodically ordered lockdowns, I analyzed accidents during weekends of April, when citizens had total movement restrictions; results are drastic decrease of accidents. In conclusion, government measures had impanct on this anomaly.

# COMMAND ----------

# MAGIC %md
# MAGIC In *Novi Beograd*, accidents occur more often than in *Voždovac* or *Palilula*.<br>
# MAGIC Number of accidents is much higher during october or december than other months, whereas in july is the lowest. For that reason, I check if there are some deviations in weater condition during those months.

# COMMAND ----------

# MAGIC %sql
# MAGIC select month(accident) as month, count(accident) as accidents, round(mean(temperature)) as mean
# MAGIC from acc_obs 
# MAGIC where year(accident)>=2016 and municipality="NOVI BEOGRAD"
# MAGIC group by month 
# MAGIC order by mean

# COMMAND ----------

# MAGIC %sql
# MAGIC select condition, count(condition) as accidents
# MAGIC from acc_obs
# MAGIC where not (month(accident)=10 or month(accident)=12) and year(accident)>=2016 and municipality="NOVI BEOGRAD"
# MAGIC group by condition
# MAGIC order by condition

# COMMAND ----------

# MAGIC %sql
# MAGIC select condition, count(condition) as accidents
# MAGIC from acc_obs
# MAGIC where (month(accident)=10 or month(accident)=12) and year(accident)>=2016 and municipality="NOVI BEOGRAD"
# MAGIC group by condition
# MAGIC order by condition

# COMMAND ----------

# MAGIC %sql
# MAGIC select condition, count(condition) as occured
# MAGIC from observations
# MAGIC where (month(timestamp)=10 or month(timestamp)=12) and year(timestamp)>=2016
# MAGIC group by condition

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.condition, count(a.condition) as accidents
# MAGIC from acc_obs as a
# MAGIC where (month(accident)=10 or month(accident)=12) and year(accident)>=2016 and municipality="NOVI BEOGRAD"
# MAGIC group by a.condition
# MAGIC order by a.condition

# COMMAND ----------

# MAGIC %md
# MAGIC Number of accidents in *Novi Beograd* by weather conditions during oktober and december in oppose to other months:<br>
# MAGIC Windy 43-172<br>
# MAGIC Snow 62-192<br>
# MAGIC Rain 204-690<br>
# MAGIC Fog 358-692<br>
# MAGIC Cloudy 548-1727<br>
# MAGIC Fair 1282-6759<br>
# MAGIC <br>
# MAGIC Finding that there is no significant deviations in temperature or number of accidents by weather conditions, I conclude that weather has no important impact on frequency of accidents.

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.municipality, count(a.outcome) as accidents, i.num as injuries, ROUND(count(a.outcome)/i.num,2) as iratio, 
# MAGIC                                                       d.num as deaths, ROUND(count(a.outcome)/d.num,2) as dratio
# MAGIC from accidents as a
# MAGIC inner join (select municipality, count(outcome) as num
# MAGIC             from accidents
# MAGIC             where outcome="death" and year(timestamp)>=2016
# MAGIC             group by municipality) as d
# MAGIC on a.municipality=d.municipality
# MAGIC inner join (select municipality, count(outcome) as num
# MAGIC             from accidents
# MAGIC             where outcome="injury" and year(timestamp)>=2016
# MAGIC             group by municipality) as i
# MAGIC on a.municipality=i.municipality
# MAGIC where year(a.timestamp)>=2016
# MAGIC group by a.municipality, d.num, i.num
# MAGIC order by dratio

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select day_time, count(id) as number
# MAGIC from accidents
# MAGIC where year(timestamp)>=2016 and municipality="LAZAREVAC" and outcome="death"
# MAGIC group by day_time
# MAGIC order by number desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.municipality, count(a.outcome) as accidents, i.num as injuries, ROUND(count(a.outcome)/i.num,2) as iratio, 
# MAGIC                                                       d.num as deaths, ROUND(count(a.outcome)/d.num,2) as dratio
# MAGIC from accidents as a
# MAGIC inner join (select municipality, count(outcome) as num
# MAGIC             from accidents
# MAGIC             where outcome="death" and year(timestamp)>=2016 and day_time="afternoon"
# MAGIC             group by municipality) as d
# MAGIC on a.municipality=d.municipality
# MAGIC inner join (select municipality, count(outcome) as num
# MAGIC             from accidents
# MAGIC             where outcome="injury" and year(timestamp)>=2016 and day_time="afternoon"
# MAGIC             group by municipality) as i
# MAGIC on a.municipality=i.municipality
# MAGIC where year(a.timestamp)>=2016 and day_time="afternoon"
# MAGIC group by a.municipality, d.num, i.num
# MAGIC order by dratio

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table acc_daytime;
# MAGIC create table acc_daytime
# MAGIC select municipality, day_time, count(id) as num
# MAGIC from accidents
# MAGIC where year(timestamp)>=2016
# MAGIC group by municipality, day_time
# MAGIC order by municipality, day_time

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.municipality, a.day_time, t.max
# MAGIC from acc_daytime as a
# MAGIC inner join  (select municipality, max(num) as max
# MAGIC             from acc_daytime
# MAGIC             group by municipality) as t
# MAGIC on a.municipality=t.municipality and a.num=t.max
# MAGIC order by a.municipality

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.municipality, count(a.outcome) as accidents, i.num as afternoon, ROUND(count(a.outcome)/i.num,2) as ratio
# MAGIC from accidents as a
# MAGIC inner join (select municipality, count(id) as num
# MAGIC             from accidents
# MAGIC             where day_time="afternoon" and year(timestamp)>=2016
# MAGIC             group by municipality) as i
# MAGIC on a.municipality=i.municipality
# MAGIC where year(a.timestamp)>=2016
# MAGIC group by a.municipality, i.num
# MAGIC order by ratio

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.municipality, count(a.outcome) as accidents, p.num as `pedestrian involved`, ROUND(count(a.outcome)/p.num) as ratio,
# MAGIC                                                       d.num as dnumber, ROUND(p.num/d.num) as dratio
# MAGIC from accidents as a
# MAGIC inner join (select municipality, count(outcome) as num
# MAGIC             from accidents
# MAGIC             where participants="pedestrian" and year(timestamp)>=2016
# MAGIC             group by municipality) as p
# MAGIC on a.municipality=p.municipality
# MAGIC inner join (select municipality, count(outcome) as num
# MAGIC             from accidents
# MAGIC             where participants="pedestrian" and year(timestamp)>=2016 and outcome="death"
# MAGIC             group by municipality) as d
# MAGIC on a.municipality=d.municipality
# MAGIC where year(a.timestamp)>=2016
# MAGIC group by a.municipality, p.num, d.num
# MAGIC order by dratio, ratio

# COMMAND ----------

# MAGIC %md
# MAGIC Surprisingly, municipalties with the least number of accidents are much more risky; *Lazarevac* (1507) and *Barajevo* (885) are the most risky, where every 33td and 44th accident respectively, ends with death. *Barajevo* is also the most dangerous for pedestrians where every 12th accident includes pedestrian and every 111th results in death.<br>
# MAGIC On the other hand, I consider *Vracar* as safest municipality, where in 5121 accidents, only 8 resulted in death.<br>
# MAGIC Also, afternoon (14-18h) is the period of day with the highest number of accidents and the most difficult outcomes.  
