# Databricks notebook source
# MAGIC %run "/covid_reporting/Includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,StringType,IntegerType,DoubleType,DataType
from pyspark.sql.functions import col,min,max,split,concat,lit,regexp_replace


# COMMAND ----------

# Define the Azure SQL database connection details
jdbc_url = "jdbc:sqlserver://covid-sam-srv.database.windows.net:1433"
jdbc_username = "adm"
jdbc_password = dbutils.secrets.get(scope="covid-scope", key= 'azure-sql-password')
jdbc_sql_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

# COMMAND ----------

# Read the latest version of the Delta Lake table
delta_table_read_df = spark.read.format("delta").load(f"{preprocessed_adls_folder_path}/ecdc/testing")

# COMMAND ----------

delta_table_read_df.columns

# COMMAND ----------

delta_table= delta_table_read_df.select(col('country'),
 col('country_code_2_digit'),
 col('country_code_3_digit'),
 col('year_week'),
 col('reported_week_start_date').alias('week_start_date'),
 col('reported_week_end_date').alias('week_end_date'),
 col('new_cases'),
 col('tests_done'),
 col('population'),
 col('testing_data_source'))

# COMMAND ----------

# Write the Delta Lake table to the Azure SQL database
jdbc_database="covid-db"
jdbc_table = "covid_reporting.testing"
delta_table.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("databaseName",jdbc_database)\
    .option("dbtable", jdbc_table) \
    .option("user", jdbc_username) \
    .option("password", jdbc_password) \
    .option("driver", jdbc_sql_driver) \
    .mode("overwrite") \
    .save()



