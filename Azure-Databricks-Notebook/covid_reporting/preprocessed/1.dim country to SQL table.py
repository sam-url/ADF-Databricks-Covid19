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
cntry_lkp_table_read_df = spark.read.format("csv").option('header',True).load(f"{raw_adls_folder_path}/country_lookup.csv")

# COMMAND ----------

# Read the latest version of the Delta Lake table
dim_dt_table_read_df = spark.read.format("csv").option('header',True).load(f"{raw_adls_folder_path}/dim_date.csv")

# COMMAND ----------

# delta_table = delta_table_read_df.select(col('country'),
#  col('country_code_2_digit'),
#  col('country_code_3_digit'),
#  col('confirmed_cases').alias('cases_count'),
#  col('deaths').alias('deaths_count'),
#  col('reported_date'),
#  col('source'))

# COMMAND ----------

# Write the Delta Lake table to the Azure SQL database
jdbc_database="covid-db"
jdbc_table = "covid_reporting_dim.lkp_cntry"
cntry_lkp_table_read_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("databaseName",jdbc_database)\
    .option("dbtable", jdbc_table) \
    .option("user", jdbc_username) \
    .option("password", jdbc_password) \
    .option("driver", jdbc_sql_driver) \
    .mode("overwrite") \
    .save()

    #.option("createTableColumnTypes", "ID, Name, ...") \

# COMMAND ----------

# Write the Delta Lake table to the Azure SQL database
jdbc_database="covid-db"
jdbc_table = "covid_reporting_dim.dim_dt"
dim_dt_table_read_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("databaseName",jdbc_database)\
    .option("dbtable", jdbc_table) \
    .option("user", jdbc_username) \
    .option("password", jdbc_password) \
    .option("driver", jdbc_sql_driver) \
    .mode("overwrite") \
    .save()


