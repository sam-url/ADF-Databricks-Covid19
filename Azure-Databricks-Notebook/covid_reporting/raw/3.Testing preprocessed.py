# Databricks notebook source
# MAGIC %run "/covid_reporting/Includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,StringType,IntegerType,DoubleType,DataType
from pyspark.sql.functions import col,min,max,split,concat,lit,regexp_replace


# COMMAND ----------

testing_schema=StructType(
    [
        StructField('country',StringType(),True),
        StructField('country_code',StringType(),True),        
        StructField('year_week',StringType(),True),
        StructField('new_cases',IntegerType(),True),
        StructField('tests_done',StringType(),True),
        StructField('population',IntegerType(),True),
        StructField('testing_rate',DoubleType(),True),
        StructField('positivity_rate',DoubleType(),True),
        StructField('testing_data_source',StringType(),True)
    ]
)

# COMMAND ----------

# #Reading hospital_admissions,csv file
# rd_df=spark.read.option('header',True).option('inferSchema',True).csv(f'{raw_adls_folder_path}/ecdc/testing.csv')

# COMMAND ----------

#Reading hospital_admissions,csv file
read_df=spark.read.option('header',True).schema(testing_schema).csv(f'{raw_adls_folder_path}/ecdc/testing.csv')

# COMMAND ----------

#Reading lkp country_lookup.csv file
lkp_df=spark.read.option('inferSchema',True).option('header',True).csv(f'{raw_adls_folder_path}/country_lookup.csv')

# COMMAND ----------

#join lkp and hospital file to get country_2_digit and country_3_digit
jn_df=read_df.join(lkp_df,(read_df.country==lkp_df.country),'left').drop(lkp_df.country,lkp_df.continent,lkp_df.population,read_df.country_code)

# COMMAND ----------

jn_df.count()

# COMMAND ----------

#Read dim_dt file
dim_dt_df=spark.read.option('inferSchema',True).option('header',True).csv(f'{raw_adls_folder_path}/dim_date.csv')

# COMMAND ----------

yr_wk_dim_dt_df=dim_dt_df.groupBy('year','week_of_year').agg(min('date').alias('reported_week_start_date'),max('date').alias('reported_week_end_date'))

# COMMAND ----------

# %sql
# use cvd_raw;
# with cte as (
# select min(date),max(date) from cvd_raw.dim_date group by year,week_of_year
# )
# select count(*) from cte;

# COMMAND ----------

#join group by df with wkly_df to get reported_week_start date and end date
jn_wkly_dim_dt_df=jn_df.join(yr_wk_dim_dt_df,(split(jn_df.year_week, '-').getItem(0) == yr_wk_dim_dt_df.year) & (regexp_replace(split(jn_df.year_week, '-').getItem(1),'^[A-Z][0]|^[A-Z]','')==yr_wk_dim_dt_df.week_of_year),'left')

# COMMAND ----------

df=jn_wkly_dim_dt_df.drop('year', 'week_of_year',)

# COMMAND ----------

final_df=df.select ([col(clmn).alias(clmn.replace(' ', '_')) for clmn in df.columns]).orderBy(col('reported_week_start_date'),col('country_code_2_digit'))


# COMMAND ----------

#display(final_df.filter(col('year_week')=='2020-W01'))

# COMMAND ----------

# for clmn in final_dly_df.columns:
#     print(clmn) 
#     print(clmn.replace(' ','_'))

# COMMAND ----------

final_df.write.mode('overwrite').format("delta").save(f'{preprocessed_adls_folder_path}/ecdc/testing')


# COMMAND ----------

# loading data as csv to move further to sql DB to create BI report
final_df.write.mode('overwrite').format("csv").save(f'{presentation_adls_folder_path}/ecdc/testing')


# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists cvd_preprocessed.testing
# MAGIC using delta
# MAGIC location '/mnt/covid19reportingsamdl/preprocessed/ecdc/testing'

# COMMAND ----------

# %sql
# select count(*) from cvd_preprocessed.testing
