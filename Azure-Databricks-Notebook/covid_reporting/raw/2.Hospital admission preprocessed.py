# Databricks notebook source
# MAGIC %run "/covid_reporting/Includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,StringType,IntegerType,DoubleType,DateType
from pyspark.sql.functions import col,min,max,split,concat,lit


# COMMAND ----------

hospital_admissions_schema=StructType(
    [
        StructField('country',StringType(),True),
        StructField('indicator',StringType(),True),
        StructField('date',DateType(),True),
        StructField('year_week',StringType(),True),
        StructField('value',DoubleType(),True),
        StructField('source',StringType(),True),
        StructField('url',StringType(),True)
    ]
)

# COMMAND ----------

#Reading hospital_admissions,csv file
rd_df=spark.read.option('header',True).schema(hospital_admissions_schema).csv(f'{raw_adls_folder_path}/ecdc/hospital_admissions.csv')

# COMMAND ----------

read_df=rd_df.withColumnRenamed('date','reported_date').withColumnRenamed('year_week','reported_year_week')

# COMMAND ----------

#Reading lkp country_lookup.csv file
lkp_df=spark.read.option('inferSchema',True).option('header',True).csv(f'{raw_adls_folder_path}/country_lookup.csv')

# COMMAND ----------

#join lkp and hospital file to get country_2_digit and country_3_digit
jn_df=read_df.join(lkp_df,(read_df.country==lkp_df.country),'left').drop(lkp_df.country,lkp_df.continent,read_df.url)

# COMMAND ----------

#conditional Split
dly_df=jn_df.filter((col('indicator')=="Daily hospital occupancy") | (col('indicator')=="Daily ICU occupancy")).drop(col('reported_year_week'))
wkly_df=jn_df.filter((col('indicator')=="Weekly new hospital admissions per 100k") | (col('indicator')=="Weekly new ICU admissions per 100k")).drop(col('reported_date'))

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
jn_wkly_dim_dt_df=wkly_df.join(yr_wk_dim_dt_df,(split(wkly_df.reported_year_week, '-').getItem(0) == yr_wk_dim_dt_df.year) & (split(wkly_df.reported_year_week, '-').getItem(1)==concat(lit('W'),yr_wk_dim_dt_df.week_of_year)),'left')

# COMMAND ----------

slct_wkly_df=jn_wkly_dim_dt_df.select('country','country_code_2_digit','country_code_3_digit','population','reported_year_week','reported_week_start_date','reported_week_end_date','indicator','value','source')

# COMMAND ----------

pvt_dly_df=dly_df.groupBy('country','country_code_2_digit','country_code_3_digit','population','reported_date','source').pivot('indicator').sum('value')

# COMMAND ----------

#dly_df.filter((col('country_code_2_digit')=='DK') & (col('reported_date')=='2020-09-30')).show(5)

# COMMAND ----------

#pvt_dly_df.filter((col('country_code_2_digit')=='DK') & (col('reported_date')=='2020-09-30')).show(5)

# COMMAND ----------

pvt_slct_wkly_df=slct_wkly_df.groupBy('country','country_code_2_digit','country_code_3_digit','population','reported_year_week','reported_week_start_date','reported_week_end_date','source').pivot('indicator').sum('value')

# COMMAND ----------

final_dly_df=pvt_dly_df.select ([col(clmn).alias(clmn.replace(' ', '_')) for clmn in pvt_dly_df.columns]).orderBy(col('reported_date'),col('country_code_2_digit'))


# COMMAND ----------

final_wkly_df=pvt_slct_wkly_df.select ([col(clmn).alias(clmn.replace(' ', '_')) for clmn in pvt_slct_wkly_df.columns]).orderBy(col('reported_week_start_date'),col('country_code_2_digit'))


# COMMAND ----------

# for clmn in final_dly_df.columns:
#     print(clmn) 
#     print(clmn.replace(' ','_'))

# COMMAND ----------

final_dly_df.write.mode('overwrite').format("delta").save(f'{preprocessed_adls_folder_path}/ecdc/hospital_admissions_daily')


# COMMAND ----------

# loading data as csv to move further to sql DB to create BI report
final_dly_df.write.mode('overwrite').format("csv").save(f'{presentation_adls_folder_path}/ecdc/hospital_admissions_daily')


# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists cvd_preprocessed.hospital_admissions_daily
# MAGIC using delta
# MAGIC location '/mnt/covid19reportingsamdl/preprocessed/ecdc/hospital_admissions_daily'

# COMMAND ----------

final_wkly_df.write.mode('overwrite').format("delta").save(f'{preprocessed_adls_folder_path}/ecdc/hospital_admissions_weekly')

# COMMAND ----------

# loading data as csv to move further to sql DB to create BI report
final_wkly_df.write.mode('overwrite').format("csv").save(f'{presentation_adls_folder_path}/ecdc/hospital_admissions_weekly')


# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists cvd_preprocessed.hospital_admissions_weekly
# MAGIC using delta
# MAGIC location '/mnt/covid19reportingsamdl/preprocessed/ecdc/hospital_admissions_weekly'

# COMMAND ----------

