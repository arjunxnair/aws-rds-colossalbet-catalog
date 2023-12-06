# Databricks notebook source
# MAGIC %md
# MAGIC # Network visualization with D3.js
# MAGIC ---
# MAGIC One quick way to identify topics in tweets is to analyze the use of keywords or hashtags systematically. A common approach is to find pairs of hashtags (or words) that are often mentioned together in the same tweets. Visualizing the result such that pairs that often appear together are drawn close to each other gives us a visual way to explore a topic map.
# MAGIC
# MAGIC In this notebook, you'll learn how to quickly visualize networks using D3.js.
# MAGIC
# MAGIC You can find a good introduction to the force layout with D3.js here: <a href="https://www.d3indepth.com/force-layout/" target="_blank">Force layout on d3indepth.com</a>
# MAGIC
# MAGIC Find a collection of examples of what you can do with D3.js here: <a href="https://observablehq.com/@d3/gallery" target="_blank">D3.js gallery</a>

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS aws_rds_colossalbet.analytics.user_ip;
# MAGIC
# MAGIC CREATE TABLE aws_rds_colossalbet.analytics.user_ip AS
# MAGIC SELECT  trim(REPLACE(UserIP, '&quot;', '')) as UserIP, UserID, AccountID, count(1) as counts
# MAGIC FROM aws_rds_colossalbet.dbo.warehouse_taxledger_snapshot where UserIP is not null and LEN(trim(UserIP))> 5
# MAGIC group by trim(REPLACE(UserIP, '&quot;', '')), UserID, AccountID;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create a view for hashtag pairs

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explode hashtag array and create a view

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view aws_rds_colossalbet.analytics.nodes_filtered as
# MAGIC
# MAGIC select * from aws_rds_colossalbet.analytics.user_ip where UserIP not in (select UserIP from aws_rds_colossalbet.analytics.user_ip group by UserIP having count(distinct UserID) >=10)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view aws_rds_colossalbet.analytics.nodes as
# MAGIC select UserID as `id`
# MAGIC       ,10 as `size`
# MAGIC       ,'user' as type
# MAGIC from aws_rds_colossalbet.analytics.nodes_filtered
# MAGIC
# MAGIC UNION 
# MAGIC
# MAGIC select UserIP as `id`
# MAGIC       ,2 as `size`
# MAGIC       ,'ip' as type
# MAGIC from aws_rds_colossalbet.analytics.nodes_filtered
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view aws_rds_colossalbet.analytics.edges as
# MAGIC select 
# MAGIC       UserID as `source`
# MAGIC      ,UserIP as `target`
# MAGIC      ,counts as `weight`
# MAGIC from aws_rds_colossalbet.analytics.nodes_filtered
# MAGIC

# COMMAND ----------


