# Databricks notebook source
from pyspark.sql import SparkSession
colossalbet_con = f"jdbc:sqlserver://colossalbet.calle8u3juvv.ap-southeast-2.rds.amazonaws.com:1433;databaseName=colossalbet;user=admin;password=zxkxAQgQ97F4f1zvXdZx;encrypt=true;trustServerCertificate=true"

# COMMAND ----------

remote_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.dbo.Client_DIM")
target_table = "aws_rds_colossalbet.dbo.Client_DIM"
remote_table.write.mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

remote_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.dbo.Sport_DIM")
target_table = "aws_rds_colossalbet.dbo.Sport_DIM"
remote_table.write.mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

remote_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.dbo.Status_DIM")
target_table = "aws_rds_colossalbet.dbo.Status_DIM"
remote_table.write.mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

remote_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.dbo.BetMethod_DIM")
target_table = "aws_rds_colossalbet.dbo.BetMethod_DIM"
remote_table.write.mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

remote_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.dbo.BetType_DIM")
target_table = "aws_rds_colossalbet.dbo.BetType_DIM"
remote_table.write.mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

remote_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.dbo.Affiliates_DIM")
target_table = "aws_rds_colossalbet.dbo.Affiliates_DIM"
remote_table.write.mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

remote_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.dbo.BonusBetPromotions_DIM")
target_table = "aws_rds_colossalbet.dbo.BonusBetPromotions_DIM"
remote_table.write.mode("overwrite").saveAsTable(target_table)

# COMMAND ----------


