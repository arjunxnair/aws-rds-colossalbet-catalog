# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from datetime import datetime, timedelta

# COMMAND ----------

colossalbet_con = f"jdbc:sqlserver://colossalbet.calle8u3juvv.ap-southeast-2.rds.amazonaws.com:1433;databaseName=colossalbet;user=admin;password=zxkxAQgQ97F4f1zvXdZx;encrypt=true;trustServerCertificate=true"

# COMMAND ----------

start_date = datetime.strptime('2023-11-23', '%Y-%m-%d')
end_date = datetime.strptime('2023-11-23', '%Y-%m-%d')
loop_date = start_date

# COMMAND ----------

while loop_date <= end_date:

  print(loop_date)
  
  # Loop through each day in the month

  #'''
  ### Transactions_FACT | Primary Key : PK_TransactionID  and Date used is TransactionDate ### 
  # Filter remote table to the loop day
  remote_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.dbo.Transactions_FACT")
  target_table = "aws_rds_colossalbet.dbo.Transactions_FACT_stg"
  remote_table_day= remote_table.filter((to_date(col("TransactionDate"))==loop_date))

  # Write remote table to the staging table in DBR catalog
  remote_table_day.write.mode("overwrite").saveAsTable(target_table)

  # Merge the staging table to prod table
  merge_staging_to_prod_query = f"""
  MERGE INTO aws_rds_colossalbet.dbo.Transactions_FACT AS target
    USING aws_rds_colossalbet.dbo.Transactions_FACT_stg AS source
    ON target.PK_TransactionID = source.PK_TransactionID
  WHEN MATCHED THEN
    UPDATE SET *
  WHEN NOT MATCHED THEN
    INSERT *
    """
  # Print results of the merge
  display(spark.sql(merge_staging_to_prod_query))
  #'''


  #'''
  ### BetsAndReturns_FACT | Primary Key : BetsAndReturnsID and date column used is Finalised ### 
  # Filter remote table to the loop day
  remote_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.dbo.BetsAndReturns_FACT")
  target_table = "aws_rds_colossalbet.dbo.BetsAndReturns_FACT_stg"
  remote_table_day= remote_table.filter((to_date(col("Finalised"))==loop_date)| (to_date(col("TransactionDate"))==loop_date))

  # Write remote table to the staging table in DBR catalog
  remote_table_day.write.mode("overwrite").saveAsTable(target_table)

  # Merge the staging table to prod table
  merge_staging_to_prod_query = f"""
  MERGE INTO aws_rds_colossalbet.dbo.BetsAndReturns_FACT AS target
    USING aws_rds_colossalbet.dbo.BetsAndReturns_FACT_stg AS source
    ON target.BetAndReturnsID = source.BetAndReturnsID
  WHEN MATCHED THEN
    UPDATE SET *
  WHEN NOT MATCHED THEN
    INSERT *
    """
  # Print results of the merge
  display(spark.sql(merge_staging_to_prod_query))
  #'''


  ### WAREHOUSE_TaxLedger_SNAPSHOT | Primary Key : WTLS_ID and date column used is Finalised ### 
  # Filter remote table to the loop day
  remote_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.dbo.WAREHOUSE_TaxLedger_SNAPSHOT")
  target_table = "aws_rds_colossalbet.dbo.WAREHOUSE_TaxLedger_SNAPSHOT_stg"
  remote_table_day= remote_table.filter((to_date(col("Finalised"))==loop_date))

  # Write remote table to the staging table in DBR catalog
  remote_table_day.write.mode("overwrite").saveAsTable(target_table)

  # Merge the staging table to prod table
  merge_staging_to_prod_query = f"""
  MERGE INTO aws_rds_colossalbet.dbo.WAREHOUSE_TaxLedger_SNAPSHOT AS target
    USING aws_rds_colossalbet.dbo.WAREHOUSE_TaxLedger_SNAPSHOT_stg AS source
    ON target.WTLS_ID = source.WTLS_ID
  WHEN MATCHED THEN
    UPDATE SET *
  WHEN NOT MATCHED THEN
    INSERT *
    """
  # Print results of the merge
  display(spark.sql(merge_staging_to_prod_query))


  #'''
  ### BonusBets_FACT | Primary Key : BonusBetID ### 
  # Filter remote table to the loop day
  remote_table = spark.read.jdbc(url=colossalbet_con, table="colossalbet.dbo.BonusBets_FACT")
  target_table = "aws_rds_colossalbet.dbo.BonusBets_FACT_stg"
  remote_table_day= remote_table.filter((to_date(col("CreatedAt"))==loop_date)|(to_date(col("ClaimedAt"))==loop_date))

  # Write remote table to the staging table in DBR catalog
  remote_table_day.write.mode("overwrite").saveAsTable(target_table)

  # Merge the staging table to prod table
  merge_staging_to_prod_query = f"""
  MERGE INTO aws_rds_colossalbet.dbo.BonusBets_FACT AS target
    USING aws_rds_colossalbet.dbo.BonusBets_FACT_stg AS source
    ON target.BonusBetID = source.BonusBetID
  WHEN MATCHED THEN
    UPDATE SET *
  WHEN NOT MATCHED THEN
    INSERT *
    """
  # Print results of the merge
  display(spark.sql(merge_staging_to_prod_query))
  #'''
  #increment the loop day
  loop_date += timedelta(days=1)

# COMMAND ----------


