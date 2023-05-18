# Databricks notebook source
# MAGIC %run "/covid_reporting/Includes/configuration"

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS cvd_preprocessed;

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,DateType
from pyspark.sql.functions import col,length,lit,current_date

# COMMAND ----------

cases_deaths_schema =StructType([StructField('country', StringType(), True),
                                    StructField('country_code', StringType(), True),
                                    StructField('continent', StringType(), True),
                                    StructField('population', IntegerType(), True),
                                    StructField('indicator', StringType(), True),
                                    StructField('daily_count', IntegerType(), True),
                                    StructField('date', DateType(), True),
                                    StructField('rate_14_day', DoubleType(), True),
                                    StructField('source', StringType(), True)                                    
                                ])


# COMMAND ----------

#from pyspark.sql.functions import col,length
#df=spark.table('cvd_raw.cases_deaths')
#cases_deaths_remove_col_df=df.filter((df.continent=='Europe')&(length(df.country_code)!=0)).select(col('country'),col('country_code'),col('population'),col('indicator'),col('daily_count'),col('date'),col('source')).withColumnRenamed('country_code','country_code_3_digit').withColumnRenamed('date','reported_date')
#cases_deaths_remove_col_df.count()

#a way to read file from managed table and apply trnasformation


# COMMAND ----------

cases_deaths_df=spark.read.option('header',True) \
 .schema(cases_deaths_schema).csv(f'{raw_adls_folder_path}/ecdc/cases_deaths.csv')

# COMMAND ----------

#display(cases_deaths_df)

# COMMAND ----------

#cases_deaths_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###selecting required column, filtering data only for Europe, and renaming columns

# COMMAND ----------

cases_deaths_remove_col_df=cases_deaths_df.filter((cases_deaths_df.continent=='Europe') & (cases_deaths_df.country_code.isNotNull())).select(col('country'),col('country_code'),col('population'),col('indicator'),col('daily_count'),col('date'),col('source')).withColumnRenamed('country_code','country_code_3_digit').withColumnRenamed('date','reported_date')

# COMMAND ----------

cases_deaths_remove_col_df.count()

# COMMAND ----------

#cases_deaths_remove_col_df.count()

# COMMAND ----------

#display(cases_deaths_remove_col_df.orderBy('country_code'))

# COMMAND ----------

#display(cases_deaths_remove_col_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Pivoting table for indicator(confirmed cases, deaths) with daily_count

# COMMAND ----------

from pyspark.pandas import pivot
#from pyspark.sql.function import groupBy


# COMMAND ----------

pvt_df=cases_deaths_remove_col_df.groupBy('country','country_code_3_digit','population','source','reported_date').pivot('indicator').sum('daily_count').withColumnRenamed('confirmed cases','confirmed_cases')

# COMMAND ----------

pvt_df.count()

# COMMAND ----------

#display(pvt_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lookup on file to get country_code_2_digit

# COMMAND ----------

lkp_df=spark.read.option('header',True) \
 .csv(f'{raw_adls_folder_path}/ecdc/country_lookup.csv')

# COMMAND ----------

lkp_df.show(5)

# COMMAND ----------

#lkp_df.printSchema()

# COMMAND ----------

jn_df=pvt_df.join(lkp_df,lkp_df.country==pvt_df.country,'left').drop(lkp_df.country,lkp_df.country_code_3_digit,lkp_df.continent,lkp_df.population)

# COMMAND ----------

jn_df.count()

# COMMAND ----------

#display(jn_df)

# COMMAND ----------

# final_df=jn_df.filter(col('country_code_2_digit')=='UK').select(col('country'),col('country_code_2_digit'),col('country_code_3_digit'),col('population'),col('source'),col('reported_date'),col('confirmed_cases'),col('deaths')).orderBy('reported_date','country')

# COMMAND ----------

final_df=jn_df.select(col('country'),col('country_code_2_digit'),col('country_code_3_digit'),col('population'),col('source'),col('reported_date'),col('confirmed_cases'),col('deaths'))\
    .withColumn('creat_dt',current_date())\
    .orderBy('reported_date','country')

# COMMAND ----------

final_df.count()

# COMMAND ----------

final_df.write.mode('overwrite').format("delta").save(f'{preprocessed_adls_folder_path}/ecdc/cases_deaths')
#circuits_final_df.write.mode('overwrite').parquet(f'{preprocessed_folder_path}/circuits')
#circuits_final_df.write.mode('overwrite').format("delta").saveAsTable('f1_preprocessed_adls.circuits')

# COMMAND ----------

dbutils.fs.ls(f'{presentation_adls_folder_path}')

# COMMAND ----------

# loading data as csv to move further to sql DB to create BI report
final_df.write.mode('overwrite').format("csv").save(f'{presentation_adls_folder_path}/ecdc/cases_deaths')


# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists cvd_preprocessed.cases_deaths;
# MAGIC create table cvd_preprocessed.cases_deaths
# MAGIC using DELTA
# MAGIC location '/mnt/covid19reportingsamdl/preprocessed/ecdc/cases_deaths'

# COMMAND ----------

# %sql
# --select distinct country_code_2_digit from cvd_preprocessed.cases_deaths order by 1
# select count(*) from cvd_preprocessed.cases_deaths

# COMMAND ----------

