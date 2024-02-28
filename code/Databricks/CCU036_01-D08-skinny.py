# Databricks notebook source
# MAGIC %md # CCU036_01-D08-skinny
# MAGIC  
# MAGIC **Description** This notebook creates the skinny table.
# MAGIC  
# MAGIC **Author(s)** adapted from the work of Tom Bolton (John Nolan, Elena Raffetti, CCU002) for ccu018

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

# DBTITLE 1,Functions_skinny
# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/skinny"

# COMMAND ----------

# DBTITLE 1,Parameters
# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU036_01/CCU036_01-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

hes_apc = spark.table(path_hes_apc)
hes_op  = spark.table(path_hes_op)
hes_ae  = spark.table(path_hes_ae)
gdppr   = spark.table(path_gdppr)
deaths  = spark.table(path_deaths)

cohort  = spark.table(path_cohort)

ethnic_hes   = spark.table(path_ethnic_hes)
ethnic_gdppr = spark.table(path_ethnic_gdppr)
lsoa_region  = spark.table(path_lsoa_region)
lsoa_imd     = spark.table(path_imd)

# COMMAND ----------

# MAGIC %md # 2 Skinny

# COMMAND ----------

# individual_censor_dates
# Note: We made the decision to change the censor date from preg_start_date to delivery_date 
#       this was to ensure that ~8% of women without gdppr or hes records on or before the preg_start_date are included
individual_censor_dates = cohort\
  .select('PERSON_ID', 'delivery_date')\
  .withColumnRenamed('delivery_date', 'CENSOR_DATE')

# check
count_var(individual_censor_dates, 'PERSON_ID'); print()
count_var(hes_apc, 'PERSON_ID_DEID'); print()

# COMMAND ----------

# MAGIC %md ## 2.1 Create

# COMMAND ----------

unassembled = skinny_unassembled(_hes_apc=hes_apc, _hes_ae=hes_ae, _hes_op=hes_op, _gdppr=gdppr, _deaths=deaths)
assembled = skinny_assembled(_unassembled=unassembled, _individual_censor_dates=individual_censor_dates, _prioritise_primary_care=0)

# COMMAND ----------

# MAGIC %md ## 2.2 Check

# COMMAND ----------

unassembled.cache()
count_var(unassembled, 'PERSON_ID'); print()

assembled.cache()
count_var(assembled, 'PERSON_ID'); print()

# COMMAND ----------

display(assembled)

# COMMAND ----------

# see skinny_checks for further checks

# COMMAND ----------

# MAGIC %md # 3 Ethnic desc

# COMMAND ----------

# MAGIC %md ## 3.1 Look up

# COMMAND ----------

# ------------------------------------------------------------------------------
# ethnic_hes
# ------------------------------------------------------------------------------
print(ethnic_hes.toPandas().head(5)); print()

tmp1 = ethnic_hes\
  .select('ETHNICITY_CODE', 'ETHNICITY_DESCRIPTION')\
  .withColumnRenamed('ETHNICITY_CODE', 'ETHNIC')\
  .withColumnRenamed('ETHNICITY_DESCRIPTION', 'ETHNIC_DESC_HES')

count_var(tmp1, 'ETHNIC'); print()
print(tmp1.toPandas().head(5)); print()


# ------------------------------------------------------------------------------
# ethnic_gdppr
# ------------------------------------------------------------------------------
print(ethnic_gdppr.toPandas().head(5)); print()
tmp2 = ethnic_gdppr\
  .select('Value', 'Label')\
  .withColumnRenamed('Value', 'ETHNIC')\
  .withColumnRenamed('Label', 'ETHNIC_DESC_GDPPR')

count_var(tmp2, 'ETHNIC'); print()
print(tmp2.toPandas().head(5)); print()


# ------------------------------------------------------------------------------
# merge
# ------------------------------------------------------------------------------
tmp3 = merge(tmp1, tmp2, ['ETHNIC']); print()
tmp3 = tmp3\
  .withColumn('ETHNIC_DESC', f.coalesce(f.col('ETHNIC_DESC_HES'), f.col('ETHNIC_DESC_GDPPR')))\
  .orderBy('ETHNIC')
# .withColumn('ETHNIC_DESC', f.when(f.col('_merge') == 'both', f.col('ETHNIC_DESC_GDPPR')).otherwise(f.col('ETHNIC_DESC')))\
# .withColumn('ETHNIC_DESCx', f.concat(f.col('ETHNIC'), f.lit(' '), f.col('ETHNIC_DESC')))\

# check
# with pd.option_context('expand_frame_repr', False):
print(tmp3.toPandas().to_string()); print()
count_var(tmp3, 'ETHNIC'); print()

# tidy
tmp4 = tmp3\
  .select('ETHNIC', 'ETHNIC_DESC')

# COMMAND ----------

# MAGIC %md ## 3.2 Create & check

# COMMAND ----------

# check
count_var(tmp4, 'ETHNIC'); print()
count_var(assembled, 'ETHNIC'); print()

# merge
tmp5 = merge(assembled, tmp4, ['ETHNIC']); print()

# check
tmpt = tab(tmp5, 'ETHNIC', '_merge', var2_unstyled=1); print()
tmp = tmp4\
  .where(f.col('ETHNIC').isin(['2', '6']))
print(tmp.toPandas().to_string()); print()

# filter
tmp6 = tmp5\
  .where(f.col('_merge') != 'right_only')

# check
tmpt = tab(tmp6, 'ETHNIC_DESC', '_merge', var2_unstyled=1); print()
count_var(tmp6, 'PERSON_ID'); print()

# add ETHNIC_CAT (CCU002_01-D04)
tmp6 = tmp6\
  .withColumn('ETHNIC_CAT',\
    f.when(f.col('ETHNIC').isin(['0','A','B','C']), f.lit('White'))\
    .when(f.col('ETHNIC').isin(['1','2','3','N','M','P']), f.lit('Black or Black British'))\
    .when(f.col('ETHNIC').isin(['4','5','6','L','K','J','H']), f.lit('Asian or Asian British'))\
    .when(f.col('ETHNIC').isin(['D','E','F','G']), f.lit('Mixed'))\
    .when(f.col('ETHNIC').isin(['7','8','W','T','S','R']), f.lit('Other'))\
    .when(f.col('ETHNIC').isin(['9','Z','X']), f.lit('Unknown'))\
    .otherwise('Unknown')\
  )

# check
tmpt = tab(tmp6, 'ETHNIC_DESC', 'ETHNIC_CAT', var2_unstyled=1); print()

# tidy
ethnic = tmp6\
  .select('PERSON_ID', 'ETHNIC_DESC', 'ETHNIC_CAT')

# COMMAND ----------

# MAGIC %md # 4 Region

# COMMAND ----------

# MAGIC %md ## 4.1 Look up

# COMMAND ----------

# check
print(lsoa_region.toPandas().head(5)); print()
count_var(lsoa_region, 'lsoa_code'); print()

tmp1 = lsoa_region\
  .select('lsoa_code', 'region_name')\
  .withColumnRenamed('lsoa_code', 'LSOA')\
  .withColumnRenamed('region_name', 'region')

# check
print(tmp1.toPandas().head(5)); print()
tmpt = tab(tmp1, 'region'); print()

# COMMAND ----------

display(tmp3)

# COMMAND ----------

tmpt = tab(tmp3, '_tmp', '_merge', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md ## 4.2 Create & check

# COMMAND ----------

# check
count_var(tmp1, 'LSOA'); print()
count_var(assembled, 'LSOA'); print()

# merge
tmp2 = merge(assembled, tmp1, ['LSOA']); print()
tmp3 = tmp2\
  .where(f.col('_merge') != 'right_only')\
  .withColumn('_tmp', f.substring(f.col('LSOA'), 1, 1))

# check
tmpt = tab(tmp3, '_tmp', '_merge', var2_unstyled=1); print()
#tmp = tmp3\
 # .where(f.col('_tmp').isin(['9', 'L', 'M']))
#tmpt = tab(tmp, 'LSOA'); print() # NEEDs ATTENTION ATTRIBUTE ERROR COMMENTED OUT FOR NOW

# tidy
tmp3 = tmp3\
  .drop('_merge')\
  .withColumn('region',\
    f.when(f.col('_tmp') == 'W', 'Wales')\
    .when(f.col('_tmp') == 'S', 'Scotland')\
    .otherwise(f.col('region'))\
  )

# check
tmpt = tab(tmp3, 'region', '_tmp', var2_unstyled=1); print()
count_var(tmp3, 'PERSON_ID')

# tidy
region = tmp3\
  .select('PERSON_ID', 'region')

# COMMAND ----------

# MAGIC %md # 5 IMD

# COMMAND ----------

# MAGIC %md ## 5.1 Look up

# COMMAND ----------

# check
print(lsoa_imd.toPandas().head(5)); print()
count_var(lsoa_imd, 'LSOA_CODE_2011'); print()
tmpt = tab(lsoa_imd, 'IMD_YEAR'); print()

# filter
tmp  = lsoa_imd\
  .where(f.col('IMD_YEAR') == 2019)

# check
tmpt = tab(lsoa_imd, 'DECI_IMD'); print()

# tidy
tmp1 = lsoa_imd\
  .where(f.col('IMD_YEAR') == 2019)\
  .select('LSOA_CODE_2011', 'DECI_IMD')\
  .withColumnRenamed('LSOA_CODE_2011', 'LSOA')\
  .withColumn('IMD',
    f.when(f.col('DECI_IMD').isin([1,2]), 1)\
    .when(f.col('DECI_IMD').isin([3,4]), 2)\
    .when(f.col('DECI_IMD').isin([5,6]), 3)\
    .when(f.col('DECI_IMD').isin([7,8]), 4)\
    .when(f.col('DECI_IMD').isin([9,10]), 5)\
    .otherwise(None)\
  )\
  .drop('DECI_IMD')

# check
print(tmp1.toPandas().head(5)); print()

# COMMAND ----------

# MAGIC %md ## 5.2 Create & check

# COMMAND ----------

# check
count_var(tmp1, 'LSOA'); print()
count_var(assembled, 'LSOA'); print()

# merge
tmp2 = merge(assembled, tmp1, ['LSOA']); print()

# tidy
tmp3 = tmp2\
  .where(f.col('_merge') != 'right_only')\
  .withColumn('_tmp', f.substring(f.col('LSOA'), 1, 1))

# check
tmpt = tab(tmp3, '_tmp', '_merge', var2_unstyled=1); print()
count_var(tmp3, 'PERSON_ID'); print()

# tidy
imd = tmp3\
  .select('PERSON_ID', 'IMD')

# COMMAND ----------

# MAGIC %md # 6 Save

# COMMAND ----------

count_var(assembled, 'PERSON_ID')
count_var(ethnic, 'PERSON_ID')
count_var(region, 'PERSON_ID')
count_var(imd, 'PERSON_ID')

tmp = assembled\
  .join(ethnic, on='PERSON_ID', how='left')\
  .join(region, on='PERSON_ID', how='left')\
  .join(imd, on='PERSON_ID', how='left')

count_var(tmp, 'PERSON_ID')

# COMMAND ----------

# check
display(tmp)

# COMMAND ----------

outName = f'{proj}_out_skinny'.lower()
 

# save previous version for comparison purposes
_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
outName_pre = f'{outName}_pre{_datetimenow}'.lower()
print(outName_pre)
spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')


# COMMAND ----------

outName = f'{proj}_out_skinny'.lower()
spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
tmp.write.saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md # 7 Compare

# COMMAND ----------

# temporary step whilst developing - compare previous version
outName = f'{proj}_out_skinny'.lower()
old = spark.table(f'{dbc}.{outName}_pre20230328_094017')
new = spark.table(f'{dbc}.{outName}')
file3_differences = compare_files(old, new, ['PERSON_ID'], warningError=0)



