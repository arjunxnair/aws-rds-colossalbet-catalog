# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, countDistinct
from datetime import datetime, timedelta

# COMMAND ----------

colossalbet_con = f"jdbc:sqlserver://colossalbet.calle8u3juvv.ap-southeast-2.rds.amazonaws.com:1433;databaseName=colossalbet;user=admin;password=zxkxAQgQ97F4f1zvXdZx;encrypt=true;trustServerCertificate=true"

# COMMAND ----------

source_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.dbo.client_dim")
target_table = spark.read.table(f"aws_rds_colossalbet.dbo.client_dim")
source_df = source_table.groupBy(to_date("DateOpened").alias("DateOpened")).agg(countDistinct("AccountID").alias("AccountCount"))
target_df = target_table.groupBy(to_date("DateOpened").alias("DateOpened")).agg(countDistinct("AccountID").alias("AccountCount"))
metric_column = "AccountCount"
join_column = "DateOpened"
result_df = (source_df
             .join(target_df, (source_df[join_column] == target_df[join_column]) , how="outer")
             .select(
                source_df[join_column],
                source_df[metric_column].alias("source_metric"),
                target_df[metric_column].alias("target_metric")
                )
             .withColumn("metric_diff", col("source_metric") - col("target_metric"))
             .filter( (col("metric_diff") != 0) | (col("metric_diff").isNull()) )
)
display(result_df.orderBy(col("DateOpened").desc()))

# COMMAND ----------

source_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.dbo.Transactions_FACT")
target_table = spark.read.table(f"aws_rds_colossalbet.dbo.Transactions_FACT")
source_df = source_table.groupBy(to_date("TransactionDate").alias("TransactionDate")).agg(countDistinct("PK_TransactionID").alias("TransactionCount"))
target_df = target_table.groupBy(to_date("TransactionDate").alias("TransactionDate")).agg(countDistinct("PK_TransactionID").alias("TransactionCount"))
metric_column = "TransactionCount"
join_column = "TransactionDate"
result_df = (source_df
             .join(target_df, (source_df[join_column] == target_df[join_column]) , how="outer")
             .select(
                source_df[join_column],
                source_df[metric_column].alias("source_metric"),
                target_df[metric_column].alias("target_metric")
                )
             .withColumn("metric_diff", col("source_metric") - col("target_metric"))
             .filter( (col("metric_diff") != 0) | (col("metric_diff").isNull()) )
)
display(result_df.orderBy(col(join_column).desc()))

# COMMAND ----------

source_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.dbo.BetsAndReturns_FACT")
target_table = spark.read.table(f"aws_rds_colossalbet.dbo.BetsAndReturns_FACT")
source_df = source_table.groupBy(to_date("Finalised").alias("Finalised")).agg(countDistinct("BetAndReturnsID").alias("TransactionCount"))
target_df = target_table.groupBy(to_date("Finalised").alias("Finalised")).agg(countDistinct("BetAndReturnsID").alias("TransactionCount"))
metric_column = "TransactionCount"
join_column = "Finalised"
result_df = (source_df
             .join(target_df, (source_df[join_column] == target_df[join_column]) , how="outer")
             .select(
                source_df[join_column],
                source_df[metric_column].alias("source_metric"),
                target_df[metric_column].alias("target_metric")
                )
             .withColumn("metric_diff", col("source_metric") - col("target_metric"))
             .filter( (col("metric_diff") != 0) | (col("metric_diff").isNull()) )
)
display(result_df.orderBy(col(join_column).desc()))

# COMMAND ----------

source_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.dbo.WAREHOUSE_TaxLedger_SNAPSHOT")
target_table = spark.read.table(f"aws_rds_colossalbet.dbo.WAREHOUSE_TaxLedger_SNAPSHOT")
source_df = source_table.groupBy(to_date("Finalised").alias("Finalised")).agg(countDistinct("WTLS_ID").alias("TransactionCount"))
target_df = target_table.groupBy(to_date("Finalised").alias("Finalised")).agg(countDistinct("WTLS_ID").alias("TransactionCount"))
metric_column = "TransactionCount"
join_column = "Finalised"
result_df = (source_df
             .join(target_df, (source_df[join_column] == target_df[join_column]) , how="outer")
             .select(
                source_df[join_column],
                source_df[metric_column].alias("source_metric"),
                target_df[metric_column].alias("target_metric")
                )
             .withColumn("metric_diff", col("source_metric") - col("target_metric"))
             .filter( (col("metric_diff") != 0) | (col("metric_diff").isNull()) )
)
display(result_df.orderBy(col(join_column).desc()))

# COMMAND ----------

source_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.dbo.WAREHOUSE_TaxLedger_SNAPSHOT").filter((to_date(col("Finalised"))=='2021-07-01'))
target_table = spark.read.table(f"aws_rds_colossalbet.dbo.WAREHOUSE_TaxLedger_SNAPSHOT").filter((to_date(col("Finalised"))=='2021-07-01'))
source_df = source_table.collect()
target_df = target_table.collect()


# COMMAND ----------

source_df.count(AccountID)

# COMMAND ----------

display(source_df)

# COMMAND ----------

display(target_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aws_rds_colossalbet.dbo.WAREHOUSE_TaxLedger_SNAPSHOT where WTLS_ID = '169013'

# COMMAND ----------


