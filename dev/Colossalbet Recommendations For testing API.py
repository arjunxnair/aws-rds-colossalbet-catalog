# Databricks notebook source
colossalbet_con = f"jdbc:sqlserver://colossalbet.calle8u3juvv.ap-southeast-2.rds.amazonaws.com:1433;databaseName=colossalbet;user=admin;password=zxkxAQgQ97F4f1zvXdZx;encrypt=true;trustServerCertificate=true"

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

remote_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.analytics.customer_recommendations")

# COMMAND ----------

target_table = "aws_rds_colossalbet.analytics.customer_recommendations"

# COMMAND ----------

remote_table.write.mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from aws_rds_colossalbet.analytics.customer_recommendations

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists aws_rds_colossalbet.analytics.customer_recommendations_api_table; 
# MAGIC create table aws_rds_colossalbet.analytics.customer_recommendations_api_table as  
# MAGIC select 
# MAGIC uuid,
# MAGIC pin,
# MAGIC genweb_event_id as eventId,
# MAGIC rbhq_meeting_venue as meeting,
# MAGIC utc_date as utcDate,
# MAGIC race_number as raceNumber,
# MAGIC runner_number as runnerNumber,
# MAGIC runner_name_formatted as runnerName,
# MAGIC hour_formatted as scheduledEventStartTime,
# MAGIC utc_time as utcEventStartTime,
# MAGIC commentary_home_page as commentary,
# MAGIC recommendation_type as recommendationCategory,
# MAGIC case 
# MAGIC when recommendation_type = 'Last Run Winner' then '01' 
# MAGIC when recommendation_type = 'Missed by a whisker' then '02'
# MAGIC when recommendation_type = 'On a Hatrick' then '30' 
# MAGIC when recommendation_type = 'Top Picks' then '31' 
# MAGIC when recommendation_type = 'Last Winner Track and Distance' then '35' 
# MAGIC when recommendation_type = 'Track and Distance Top Pick' then '36' 
# MAGIC end as code_iq
# MAGIC ,DATEDIFF(SECOND, '1970-01-01', utc_time) as ttl
# MAGIC from aws_rds_colossalbet.analytics.customer_recommendations 
# MAGIC where utc_time >= GETDATE()
# MAGIC and genweb_event_id > 0 and pin > 0 and runner_number > 0
# MAGIC and uuid not in 
# MAGIC (
# MAGIC select uuid from aws_rds_colossalbet.analytics.customer_recommendations 
# MAGIC group by uuid
# MAGIC having count(1)>1
# MAGIC )
# MAGIC and recommendation_type in ('Last Run Winner','Missed by a whisker','On a Hatrick','Top Picks','Last Winner Track and Distance' , 'Track and Distance Top Pick')
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select recommendationCategory, count(1) from aws_rds_colossalbet.analytics.customer_recommendations_api_table group by recommendationCategory;
# MAGIC --select GETDATE();

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from aws_rds_colossalbet.analytics.customer_recommendations_api_table where pin = 2258 order by ttl ;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from aws_rds_colossalbet.analytics.customer_recommendations_api_table where pin = 2497 

# COMMAND ----------


