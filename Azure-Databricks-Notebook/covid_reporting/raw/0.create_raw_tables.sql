-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Create database for accesing raw layer files via external table

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC create database cvd_raw

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC show DATABASES

-- COMMAND ----------

use cvd_raw;
drop table if exists cvd_raw.cases_deaths;
CREATE TABLE if NOT EXISTS cvd_raw.cases_deaths
(
  country String,
  country_code String,
continent String,
population BIGINT,
indicator String,
daily_count INTEGER,
date STRING,
rate_14_day DECIMAL,
source String
)
using CSV
options (path "/mnt/covid19reportingsamdl/raw/ecdc/cases_deaths.csv", header True)


-- COMMAND ----------

use cvd_raw;
drop table if exists cvd_raw.country_response;
CREATE TABLE if NOT EXISTS cvd_raw.country_response
(
  country String,
  Response_measure String,
  date_start DATE,
  date_end DATE
)
using CSV
options (path "/mnt/covid19reportingsamdl/raw/ecdc/country_response.csv", header True)

-- COMMAND ----------

use cvd_raw;
drop table if exists cvd_raw.hospital_admissions;
CREATE TABLE if NOT EXISTS cvd_raw.hospital_admissions
(
  country String,
  indicator String,
  date DATE,
  year_week string,
  value BIGINT,
  source STRING,
  url STRING
)
using CSV
options (path "/mnt/covid19reportingsamdl/raw/ecdc/hospital_admissions.csv", header True)

-- COMMAND ----------

use cvd_raw;
drop table if exists cvd_raw.testing;
CREATE TABLE if NOT EXISTS cvd_raw.testing
(
  country String,
  country_code String,
  year_week STRING,
  level string,
  region STRING,
  region_name STRING,
  new_cases INT,
  tests_done INT,
  population BIGINT,
  testing_rate DECIMAL,
  positivity_rate DECIMAL,
  testing_data_source STRING
)
using CSV
options (path "/mnt/covid19reportingsamdl/raw/ecdc/testing.csv", header True)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC use cvd_raw;
-- MAGIC drop table if exists cvd_raw.dim_date;
-- MAGIC create table if not exists cvd_raw.dim_date
-- MAGIC (
-- MAGIC   date_key integer,
-- MAGIC date date,
-- MAGIC year integer,
-- MAGIC month integer,
-- MAGIC day integer,
-- MAGIC day_name string,
-- MAGIC day_of_year integer,
-- MAGIC week_of_month integer,
-- MAGIC week_of_year integer,
-- MAGIC month_name string,
-- MAGIC year_month integer,
-- MAGIC year_week integer
-- MAGIC )
-- MAGIC using csv
-- MAGIC options (path "/mnt/covid19reportingsamdl/raw/dim_date.csv",header True)

-- COMMAND ----------

