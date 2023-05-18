# Databricks notebook source
# MAGIC %run "/covid_reporting/Includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import col,lit,length,substring,split,regexp_replace
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType
from pyspark.pandas import pivot

# COMMAND ----------

pop_schema=StructType(
    [
        StructField('indic_de,geo\time',StringType(),True),
        StructField('2008',StringType(),True),
		StructField('2009',StringType(),True),
		StructField('2010',StringType(),True),
		StructField('2011',StringType(),True),
		StructField('2012',StringType(),True),
		StructField('2013',StringType(),True),
		StructField('2014',StringType(),True),
		StructField('2015',StringType(),True),
		StructField('2016',StringType(),True),
		StructField('2017',StringType(),True),
		StructField('2018',StringType(),True),
		StructField('2019',StringType(),True)
    ])

# COMMAND ----------

read_df=spark.read.schema(pop_schema).option('header',True).option('delimiter','\t').csv(f'{raw_adls_folder_path}/population/population_by_age.tsv')

# COMMAND ----------

slct_df=read_df.withColumn('age_group',regexp_replace(split(col('indic_de,geo\time'),',').getItem(0),'PC_','')).withColumn('country_cd',split(col('indic_de,geo\time'),',').getItem(1)).drop('2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018','indic_de,geo\time').withColumnRenamed('2019','percentage_2019')

# COMMAND ----------

#read country_lkp file
lkp_df=spark.read.option('inferSchema',True).option('header',True).csv(f'{raw_adls_folder_path}/country_lookup.csv')


# COMMAND ----------

#join slct_df with country_lkp
jn_df=slct_df.join(lkp_df,(slct_df.country_cd==lkp_df.country_code_2_digit),'left').drop(lkp_df.continent,lkp_df.population,slct_df.country_cd)

# COMMAND ----------

cln_df=jn_df.withColumn('percentage_2019',regexp_replace(col('percentage_2019'),'[a-z]','').cast('decimal(4,2)'))
#x_df.createOrReplaceTempView('x_view')

# COMMAND ----------

# %sql
# select distinct percentage_2019 from x_view

# COMMAND ----------

pvt_df=cln_df.groupBy('country', 'country_code_2_digit', 'country_code_3_digit').pivot('age_group').sum('percentage_2019')

# COMMAND ----------

final_df=pvt_df.filter(col('country').isNotNull()).withColumnRenamed('Y0_14','age_group_0_14')\
               .withColumnRenamed('Y15_24','age_group_15_24')\
               .withColumnRenamed('Y25_49','age_group_25_49')\
               .withColumnRenamed('Y50_64','age_group_50_64')\
               .withColumnRenamed('Y65_79','age_group_65_79')\
               .withColumnRenamed('Y80_MAX','age_group_80_MAX')\
               .orderBy('country')

# COMMAND ----------

final_df.write.mode('overwrite').format("delta").save(f'{preprocessed_adls_folder_path}/population/population_by_age')

# COMMAND ----------

# loading data as csv to move further to sql DB to create BI report
final_df.write.mode('overwrite').format("csv").save(f'{presentation_adls_folder_path}/population/population_by_age')

