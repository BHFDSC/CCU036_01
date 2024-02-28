# Databricks notebook source
# MAGIC %md # CCU036_01-D09-quality_assurance
# MAGIC  
# MAGIC **Description** This notebook creates the quality assurance table.
# MAGIC  
# MAGIC **Author(s)** Adapted from the work by Tom Bolton for CCU018 (John Nolan, Elena Raffetti, CCU002)
# MAGIC
# MAGIC
# MAGIC  
# MAGIC Description. This notebook creates a register and applies a series of quality assurance steps to a cohort of data (Skinny Cohort table) of NHS Numbers to potentially remove from analyses due to conflicting data, with reference to previous work/coding by Spiros.
# MAGIC
# MAGIC Coding Lists. Pregnancy related codes  (these were identified by searching all available codes in the GDPPR dictionary, R code for searching dictionary is included on JC GitHub) , Prostate cancer codes   (Caliber, I reviewed this list after read to snomed conversion so only contains applicable codes)

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import numpy as np

import re
import io
import datetime

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

print("Matplotlib version: ", matplotlib.__version__)
print("Seaborn version: ", sns.__version__)
_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")

# COMMAND ----------

# DBTITLE 1,Functions
# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC # 0 Parameters

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU036_01/CCU036_01-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

skinny   = spark.table(f'{dbc}.{proj}_out_skinny')
deaths   = spark.table(path_deaths_sing)
gdppr    = spark.table(path_gdppr)
# spark.sql(f""" REFRESH TABLE {path_codelist}""")
codelist = spark.table(path_codelist)


# COMMAND ----------

cohort   = spark.table(path_cohort)

# COMMAND ----------

cohort.columns

# COMMAND ----------

count_var(skinny, 'PERSON_ID'); print()
count_var(cohort, 'PERSON_ID'); print()

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# ------------------------------------------------------------------------------
# skinny
# ------------------------------------------------------------------------------
print('skinny'); print()
_skinny = skinny\
  .select('PERSON_ID', 'DOB', 'SEX', 'DOD')

# check
count_var(_skinny, 'PERSON_ID'); print()

# ------------------------------------------------------------------------------
# cohort ADDED FOR rule #9
# ------------------------------------------------------------------------------
print('cohort'); print()
_cohort = cohort\
  .select('PERSON_ID', 'preg_start_date', 'delivery_date')

# check
count_var(_cohort, 'PERSON_ID'); print()

# merge
_qa1 = merge(_skinny, _cohort, ['PERSON_ID']); print()

# check
assert _qa1.where(f.col('_merge') == 'right_only').count() == 0
count_var(_qa1, 'PERSON_ID'); print()

# tidy
_qa1 = _qa1\
  .withColumn('in_deaths', f.when(f.col('_merge') == 'both', 1).otherwise(0))\
  .drop('_merge')

# COMMAND ----------

_qa1 = _qa1\
  .drop('in_deaths')

# COMMAND ----------

display(_qa1)

# COMMAND ----------



# ------------------------------------------------------------------------------
# deaths
# ------------------------------------------------------------------------------
print('deaths'); print()
_deaths = deaths\
  .select('PERSON_ID', 'REG_DATE', 'REG_DATE_OF_DEATH')

# check
count_var(_deaths, 'PERSON_ID'); print()

# merge
_qa = merge(_qa1, _deaths, ['PERSON_ID']); print()

# check
assert _qa.where(f.col('_merge') == 'right_only').count() == 0
count_var(_qa, 'PERSON_ID'); print()

# tidy
_qa = _qa\
  .withColumn('in_deaths', f.when(f.col('_merge') == 'both', 1).otherwise(0))\
  .drop('_merge')


# ------------------------------------------------------------------------------
# gdppr
# ------------------------------------------------------------------------------
print('gdppr'); print()
count_var(gdppr, 'NHS_NUMBER_DEID'); print()

_gdppr = gdppr\
  .select(['NHS_NUMBER_DEID', 'DATE', 'CODE'])\
  .withColumnRenamed('NHS_NUMBER_DEID', 'PERSON_ID')\
  .where(f.col('DATE').isNotNull())

# check
count_var(_gdppr, 'PERSON_ID'); print()

# COMMAND ----------

# check
display(_qa)

# COMMAND ----------

# check DOD agreement
tmp = _qa\
  .withColumn('equality', f.col('DOD') == f.col('REG_DATE_OF_DEATH'))\
  .withColumn('null_safe_equality', udf_null_safe_equality('DOD', 'REG_DATE_OF_DEATH'))
tmpt = tab(tmp, 'equality', 'null_safe_equality', var2_unstyled=1); print()

tmp1 = tmp\
  .where(f.col('DOD').isNull())
tmpt = tab(tmp1, 'REG_DATE_OF_DEATH'); print()

tmp2 = tmp\
  .where(f.col('REG_DATE_OF_DEATH').isNull())
tmpt = tab(tmp2, 'DOD'); print()

# COMMAND ----------

# check
display(_gdppr)

# COMMAND ----------

# MAGIC %md # 3 Medical conditions

# COMMAND ----------

# MAGIC %md ## 3.1 Codelist

# COMMAND ----------

# codelist hx
codelist_hx = codelist\
  .where(f.col('name').isin(['pregnancy', 'prostate_cancer']))

# check
tmpt = tab(codelist_hx, 'name', 'terminology', var2_unstyled=1); print()
_list_terms = list(\
  codelist_hx\
    .select('terminology')\
    .distinct()\
    .toPandas()['terminology']\
  )
# assert set(_list_terms) <= set(['SNOMED'])

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# AssertionError above, there now exists ICD10, OPCS4 codes for pregnancy
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
codelist_hx = codelist_hx\
  .where(f.col('terminology') == 'SNOMED')


# codelist hx gdppr
codelist_hx_gdppr = codelist_hx

# COMMAND ----------

# MAGIC %md ## 3.2 Create

# COMMAND ----------

# gdppr
_tmp_codelist_hx_gdppr = codelist_hx_gdppr\
  .select(['code', 'name'])
_hx_gdppr = _gdppr\
  .join(_tmp_codelist_hx_gdppr, on='code', how='inner')

# first event of each name
_win = Window\
  .partitionBy(['PERSON_ID', 'name'])\
  .orderBy('DATE')
_hx_1st = _hx_gdppr\
  .withColumn('_rownum', f.row_number().over(_win))\
  .where(f.col('_rownum') == 1)\
  .select('PERSON_ID', 'DATE', 'name')\
  .orderBy('PERSON_ID', 'DATE', 'name')
# note: ignore ties for source for now...

# COMMAND ----------

display(_hx_1st)

# COMMAND ----------

tab(_hx_1st, 'name')

# COMMAND ----------

# reshape
_hx_1st_wide = _hx_1st\
  .withColumn('name', f.concat(f.lit('_'), f.lower(f.col('name'))))\
  .groupBy('PERSON_ID')\
  .pivot('name')\
  .agg(f.first('DATE'))\
  .withColumn('_pregnancy_ind', f.when(f.col('_pregnancy').isNotNull(), 1))\
  .orderBy('PERSON_ID')

# COMMAND ----------

# gdppr
_tmp_codelist_hx_gdppr = codelist_hx_gdppr\
  .select(['code', 'name'])
_hx_gdppr = _gdppr\
  .join(_tmp_codelist_hx_gdppr, on='code', how='inner')

# first event of each name
_win = Window\
  .partitionBy(['PERSON_ID', 'name'])\
  .orderBy('DATE')
_hx_1st = _hx_gdppr\
  .withColumn('_rownum', f.row_number().over(_win))\
  .where(f.col('_rownum') == 1)\
  .select('PERSON_ID', 'DATE', 'name')\
  .orderBy('PERSON_ID', 'DATE', 'name')
# note: ignore ties for source for now...

# reshape
_hx_1st_wide = _hx_1st\
  .withColumn('name', f.concat(f.lit('_'), f.lower(f.col('name'))))\
  .groupBy('PERSON_ID')\
  .pivot('name')\
  .agg(f.first('DATE'))\
  .withColumn('_pregnancy_ind', f.when(f.col('_pregnancy').isNotNull(), 1))\
  .orderBy('PERSON_ID')

# COMMAND ----------

# MAGIC %md ## 3.3 Check

# COMMAND ----------

display(_hx_1st_wide)

# COMMAND ----------

count_var(_hx_1st_wide, 'PERSON_ID'); print()
count_var(_hx_1st_wide, '_pregnancy'); print()


# COMMAND ----------

# MAGIC %md # 4 Rules

# COMMAND ----------

# MAGIC %md ## 4.1 Prepare

# COMMAND ----------

# check
count_var(_qa, 'PERSON_ID'); print()
count_var(_hx_1st_wide, 'PERSON_ID'); print()
_qa = merge(_qa, _hx_1st_wide, ['PERSON_ID']); print()

# check
assert _qa.where(f.col('_merge') == 'right_only').count() == 0
count_var(_qa, 'PERSON_ID'); print()

# tidy
_qa = _qa\
  .drop('_merge')

# COMMAND ----------

# check
display(_qa)

# COMMAND ----------

# ------------------------------------------------------------------------------
# preparation: rule 8 (Patients have all missing record_dates and dates)
# ------------------------------------------------------------------------------
print('gdppr'); print()
count_var(gdppr, 'NHS_NUMBER_DEID')

# identify records with null date
_gdppr_null = gdppr\
  .select(['NHS_NUMBER_DEID', 'DATE', 'RECORD_DATE'])\
  .withColumnRenamed('NHS_NUMBER_DEID', 'PERSON_ID')\
  .withColumn('_null',\
    f.when(\
      ((f.col('DATE').isNotNull()) | (f.col('DATE') == '') | (f.col('DATE') == ' '))\
      | ((f.col('RECORD_DATE').isNotNull()) | (f.col('RECORD_DATE') == '') | (f.col('RECORD_DATE') == ' '))\
    , 0).otherwise(1)\
  )\

# check
tmpt = tab(_gdppr_null, '_null'); print()

# summarise per individual
_gdppr_null_summ = _gdppr_null\
  .groupBy('PERSON_ID')\
  .agg(\
    f.sum(f.when(f.col('_null') == 0, 1).otherwise(0)).alias('_n_gdppr_notnull')\
    , f.sum(f.col('_null')).alias('_n_gdppr_null')\
  )\
  .where(f.col('_n_gdppr_null') > 0)

# check
tmp = _gdppr_null_summ\
  .select('_n_gdppr_null')\
  .groupBy()\
  .sum()\
  .collect()[0][0]
print(tmp); print()

# check
print(_gdppr_null_summ.toPandas().to_string()); print()

# check
count_var(_qa, 'PERSON_ID'); print()
count_var(_gdppr_null_summ, 'PERSON_ID'); print()

# merge
_qa = merge(_qa, _gdppr_null_summ, ['PERSON_ID']); print()

# check
assert _qa.where(f.col('_merge') == 'right_only').count() == 0
count_var(_qa, 'PERSON_ID'); print()

# tidy
_qa = _qa\
  .drop('_merge')

# COMMAND ----------

# check
display(_qa)

# COMMAND ----------

_qa.columns

# COMMAND ----------

count_var(_qa, 'preg_start_date')

# COMMAND ----------

# MAGIC %md ## 4.2 Create

# COMMAND ----------

# Rule 1: Year of birth is after the year of death
# Rule 2: Patient does not have mandatory fields completed (nhs_number, sex, Date of birth)
# Rule 3: Year of Birth Predates NHS Established Year or Year is over the Current Date
# Rule 4: Remove those with only null/invalid dates of death
# Rule 5: Remove those where registered date of death before the actual date of death
# Rule 6: Pregnancy/birth codes for men

# Rule 7: Patients have all missing record_dates and dates
# Rule 8: Date of delivery is before pregnancy start dateI will start with the 
_qax = _qa\
  .withColumn('YOB', f.year(f.col('DOB')))\
  .withColumn('YOD', f.year(f.col('DOD')))\
  .withColumn('_rule_1', f.when(f.col('YOB') > f.col('YOD'), 1).otherwise(0))\
  .withColumn('_rule_2',\
    f.when(\
      (f.col('SEX').isNull()) | (~f.col('SEX').isin([1,2]))\
      | (f.col('DOB').isNull())\
      | (f.col('PERSON_ID').isNull()) | (f.col('PERSON_ID') == '') | (f.col('PERSON_ID') == ' ')\
    , 1).otherwise(0)\
  )\
  .withColumn('_rule_3',\
    f.when(\
      (f.col('YOB') < 1793) | (f.col('YOB') > datetime.datetime.today().year)\
    , 1).otherwise(0)\
  )\
  .withColumn('_rule_4',\
    f.when(\
      (f.col('in_deaths') == 1)\
      & (\
        (f.col('REG_DATE_OF_DEATH').isNull())\
        | (f.col('REG_DATE_OF_DEATH') <= f.to_date(f.lit('1900-01-01')))\
        | (f.col('REG_DATE_OF_DEATH') > f.current_date())\
      )\
    , 1).otherwise(0)\
  )\
  .withColumn('_rule_5', f.when(f.col('REG_DATE_OF_DEATH') > f.col('REG_DATE'), 1).otherwise(0))\
  .withColumn('_rule_6', f.when((f.col('SEX') == 1) & (f.col('_pregnancy_ind') == 1) , 1).otherwise(0))\
  .withColumn('_rule_7', f.when((f.col('_n_gdppr_null') > 0) & (f.col('_n_gdppr_notnull') == 0) , 1).otherwise(0))\
  .withColumn('_rule_8', f.when(f.col('preg_start_date') > f.col('delivery_date'), 1).otherwise(0))

# cache and check
_qax.cache()
count_var(_qax, 'PERSON_ID'); print()

# COMMAND ----------


_qax = _qa\
  .withColumn('YOB', f.year(f.col('DOB')))\
  .withColumn('YOD', f.year(f.col('DOD')))\
  .withColumn('_rule_1', f.when(f.col('YOB') > f.col('YOD'), 1).otherwise(0))\
  .withColumn('_rule_2',\
    f.when(\
      (f.col('SEX').isNull()) | (~f.col('SEX').isin([1,2]))\
      | (f.col('DOB').isNull())\
      | (f.col('PERSON_ID').isNull()) | (f.col('PERSON_ID') == '') | (f.col('PERSON_ID') == ' ')\
    , 1).otherwise(0)\
  )\
  .withColumn('_rule_3',\
    f.when(\
      (f.col('YOB') < 1793) | (f.col('YOB') > datetime.datetime.today().year)\
    , 1).otherwise(0)\
  )\
  .withColumn('_rule_4',\
    f.when(\
      (f.col('in_deaths') == 1)\
      & (\
        (f.col('REG_DATE_OF_DEATH').isNull())\
        | (f.col('REG_DATE_OF_DEATH') <= f.to_date(f.lit('1900-01-01')))\
        | (f.col('REG_DATE_OF_DEATH') > f.current_date())\
      )\
    , 1).otherwise(0)\
  )\
  .withColumn('_rule_5', f.when(f.col('REG_DATE_OF_DEATH') > f.col('REG_DATE'), 1).otherwise(0))\
  .withColumn('_rule_6', f.when((f.col('SEX') == 1) & (f.col('_pregnancy_ind') == 1) , 1).otherwise(0))\
  .withColumn('_rule_7', f.when((f.col('_n_gdppr_null') > 0) & (f.col('_n_gdppr_notnull') == 0) , 1).otherwise(0)) 
#.withColumn('_rule_8', f.when((f.col('SEX') == 2) & (f.col('_prostate_cancer_ind') == 1) , 1).otherwise(0))\
# cache and check
_qax.cache()
count_var(_qax, 'PERSON_ID'); print()

# COMMAND ----------

# MAGIC %md ## 4.3 Checks

# COMMAND ----------

for i in list(range(1, 7)):
  tmpt = tab(_qax, f'_rule_{i}'); print()

# COMMAND ----------

# rule patterns
_qax = _qax\
  .withColumn('_rule_concat', f.concat(*[f'_rule_{i}' for i in list(range(1, 7))]))
tmp = _qax\
  .groupBy('_rule_concat')\
  .agg(f.count(f.col('_rule_concat')).alias('n'))\
  .orderBy(f.desc('n'))

display(tmp)



# COMMAND ----------

tmp = _qax\
  .where(f.col('_rule_2') == 1)
display(tmp)

