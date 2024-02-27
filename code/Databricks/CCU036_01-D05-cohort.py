# Databricks notebook source
# MAGIC %md # CCU036_01-D05-cohort
# MAGIC  
# MAGIC **Description** This notebook creates a cohort table and extracts key information from the delivery record.
# MAGIC  
# MAGIC **Author(s)** Adapted from the work of Tom Bolton (John Nolan, Elena Raffetti) for ccu018_01

# COMMAND ----------

# MAGIC %md
# MAGIC **Output schema**
# MAGIC
# MAGIC |Varname | Varlabel| Description
# MAGIC |----------------|-------------------------------------------|---------|
# MAGIC |PERSON_ID_DEID | NHS Number (de-identified) | NA |
# MAGIC |npreg | Pregnancy number | NA |
# MAGIC |preg_start_date | Start of pregnancy | Delivery date minus gestational length (estimated to be 40 weeks where missing)  |
# MAGIC |preg_end_date | End of pregnancy | Delivery date |
# MAGIC |fu_end_date | End of follow-up | Up to 1 year and censored according to end study or subsequent pregnancy |

# COMMAND ----------

# Define the pregnancy cohort 


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

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU036_01/CCU036_01-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

# spark.sql(f"""REFRESH TABLE {path_hes_apc_mat}""")
hes_apc_mat = spark.table(path_hes_apc_mat)
gdppr_id    = spark.table(path_gdppr_id)
deaths      = get_archive_table('deaths')

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# MAGIC %md ## 2.1 hes_apc_mat

# COMMAND ----------

_hes_apc_mat = hes_apc_mat\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID_DEID_hes_apc_mat')\
  .withColumnRenamed('PERSON_ID_DEID_hes_apc', 'PERSON_ID_DEID')

# check
tmpt = tab(_hes_apc_mat, '_id_agree')

# COMMAND ----------

# check
display(_hes_apc_mat)

# COMMAND ----------

# MAGIC %md ## 2.2 gdppr_id

# COMMAND ----------

_gdppr_id = gdppr_id\
  .withColumnRenamed('NHS_NUMBER_DEID', 'PERSON_ID_DEID')

# check
count_var(_gdppr_id, 'PERSON_ID_DEID')

# COMMAND ----------

# check
display(_gdppr_id)

# COMMAND ----------

# MAGIC %md # 2.3 deaths

# COMMAND ----------

# check
count_var(deaths, 'DEC_CONF_NHS_NUMBER_CLEAN_DEID')
assert dict(deaths.dtypes)['REG_DATE'] == 'string'
assert dict(deaths.dtypes)['REG_DATE_OF_DEATH'] == 'string'

# define window for the purpose of creating a row number below as per the skinny patient table
_win = Window\
  .partitionBy('PERSON_ID')\
  .orderBy(f.desc('REG_DATE'), f.desc('REG_DATE_OF_DEATH'), f.desc('S_UNDERLYING_COD_ICD10'))

# rename ID
# remove records with missing IDs
# reformat dates
# reduce to a single row per individual as per the skinny patient table
# select columns required
# rename column ahead of reshape below
# sort by ID
_deaths = deaths\
  .withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'PERSON_ID')\
  .where(f.col('PERSON_ID').isNotNull())\
  .withColumn('REG_DATE', f.to_date(f.col('REG_DATE'), 'yyyyMMdd'))\
  .withColumn('REG_DATE_OF_DEATH', f.to_date(f.col('REG_DATE_OF_DEATH'), 'yyyyMMdd'))\
  .withColumn('_rownum', f.row_number().over(_win))\
  .where(f.col('_rownum') == 1)\
  .select(['PERSON_ID', 'REG_DATE', 'REG_DATE_OF_DEATH', 'S_UNDERLYING_COD_ICD10'] + [col for col in list(deaths.columns) if re.match(r'^S_COD_CODE_\d(\d)*$', col)])\
  .withColumnRenamed('S_UNDERLYING_COD_ICD10', 'S_COD_CODE_UNDERLYING')\
  .orderBy('PERSON_ID')\
  .select('PERSON_ID', 'REG_DATE_OF_DEATH')\
  .withColumnRenamed('REG_DATE_OF_DEATH', 'DOD')\
  .withColumn('death_flag', f.lit(1))

# check
count_var(_deaths, 'PERSON_ID')
count_var(_deaths, 'DOD')



# COMMAND ----------

display(_deaths)

# COMMAND ----------

# MAGIC %md # 3 Filter

# COMMAND ----------

# ----------------------------------------------------------------------------------
# checks
# ----------------------------------------------------------------------------------
# check the distribution of MATERNITY_EPISODE_TYPE by FYEAR
tmpt = tab(_hes_apc_mat, 'MATERNITY_EPISODE_TYPE', 'FYEAR', var2_unstyled=1); print()
# MATERNITY_EPISODE_TYPE
# 1 = Finished delivery episode
# 2 = Finished birth episode
# 3 = Finished other delivery episode
# 4 = Finished other birth episode
# 9 = Unfinished maternity episodes
# 99 = All other episodes

# check the distribution of MATERNITY_EPISODE_TYPE by EPITYPE
tmpt = tab(_hes_apc_mat, 'MATERNITY_EPISODE_TYPE', 'EPITYPE', var2_unstyled=1); print()
# EPITYPE
# 1 = General episode (anything that is not covered by the other codes)  
# 2 = Delivery episode  
# 3 = Birth episode  
# 4 = Formally detained under the provisions of mental health legislation ...
# 5 = Other delivery event 
# 6 = Other birth event


# ----------------------------------------------------------------------------------
# filter
# ----------------------------------------------------------------------------------
# exclude birth episodes (i.e., babies) and restrict to deliveries (i.e., mothers)
# ER to confirm the exclusion of unfinished maternity episodes
_hes_apc_mat_del = _hes_apc_mat\
  .where(f.col('MATERNITY_EPISODE_TYPE').isin([1]))


# ----------------------------------------------------------------------------------
# checks
# ----------------------------------------------------------------------------------
# check completeness and uniqueness of person ID and spell ID variables
# overall
count_var(_hes_apc_mat_del, 'PERSON_ID_DEID'); print()
# count_var(hes_apc_mat_del, 'SUSSPELLID')

# per financial year
fylist = ['1920', '2021', '2122']
for fy in fylist:
  tmp = _hes_apc_mat_del\
    .where(f.col('FYEAR') == fy)
  print('\n' + fy)
  count_var(tmp, 'PERSON_ID_DEID')
  # count_var(tmp, 'SUSSPELLID')

# COMMAND ----------

display(_hes_apc_mat_del)



# COMMAND ----------

# save
outName = f'{proj}_tmp_hes_apc_mat_del' # was _hes_apc_mat_del
spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
_hes_apc_mat_del.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md # 4 Clean 

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU036_01/CCU036_01-D05a-deliveries_clean"

# COMMAND ----------

path_hes_apc_mat_del_clean = f'{dbc}.{proj}_tmp_hes_apc_mat_del_clean' # was _tmp_hes_apc_mat_clean
# spark.sql(f"""REFRESH TABLE {path_hes_apc_mat_del_clean}""")
hes_apc_mat_del_clean = spark.table(path_hes_apc_mat_del_clean)

# COMMAND ----------

# check
count_var(hes_apc_mat_del_clean, 'PERSON_ID_DEID'); print()
tmpt = tab(hes_apc_mat_del_clean.where(f.col('_rownum') == 1), '_rownummax') #, '_delivery_date_diff_min_ge_210', var2_unstyled=1); print()

# check GESTAT_1 missingness by financial year
tmpt = tab(hes_apc_mat_del_clean, 'GESTAT_1', 'FYEAR', var2_unstyled=1); print()

# COMMAND ----------

# subset
vlist = ['PERSON_ID_DEID'
  , 'PERSON_ID_DEID_hes_apc_mat'
  , '_id_agree'       
  , 'EPIKEY'
  , '_rownum'
  , '_rownummax'
  , 'delivery_date'
  , 'GESTAT_1'
  , 'BIRWEIT_1'
  , 'NUMBABY'
  , 'NUMTAILB'
  , 'NUMPREG'
  , 'DELONSET'
  ]
# , 'BIRESUS_1' # now excluded - need to pull 1-9 from record
# , 'BIRSTAT_1' # as above

tmp1 = hes_apc_mat_del_clean\
  .select(vlist)

# COMMAND ----------

# MAGIC %md # 5 Dates

# COMMAND ----------

# ---------------------------------------------------------------------------------
# add DOD
# ---------------------------------------------------------------------------------
# check
count_var(tmp1, 'PERSON_ID_DEID'); print()
 
_deaths = _deaths\
  .withColumnRenamed('PERSON_ID', 'PERSON_ID_DEID')
tmp1 = merge(tmp1, _deaths, ['PERSON_ID_DEID']); print()
tmp1 = tmp1\
  .where(f.col('_merge').isin(['left_only', 'both']))\
  .drop('_merge')
 
# check
count_var(tmp1, 'PERSON_ID_DEID'); print()
 
vlist = vlist + ['death_flag', 'DOD']

# COMMAND ----------

# ---------------------------------------------------------------------------------
# add pregnancy start date
# ---------------------------------------------------------------------------------

tmp1 = tmp1\
  .withColumn('preg_start_date_estimated',\
    f.when((f.col('GESTAT_1').isNull()) | (f.col('GESTAT_1') == 99), 1).otherwise(0)\
  )\
  .withColumn('GESTAT_1_tmp',\
    f.when(f.col('preg_start_date_estimated') == 1, 40).otherwise(f.col('GESTAT_1'))\
  )\
  .withColumn('preg_start_date', f.expr('date_add(delivery_date, -7*GESTAT_1_tmp)'))

# check consistency with previous delivery date for individuals with multiple delivery events
# NOTE
#   "You can get pregnant as little as 3 weeks after the birth of a baby, even if you're breastfeeding and your periods haven't started again." 
#   https://www.nhs.uk/conditions/baby/support-and-services/sex-and-contraception-after-birth/
#   Using 3 weeks as cut off
_win = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy('_rownum')

tmp1 = tmp1\
  .withColumn('_delivery_date_lag1', f.lag(f.col('delivery_date')).over(_win))\
  .withColumn('_delivery_date_lag1_p21d', f.expr('date_add(_delivery_date_lag1, 21)'))\
  .withColumn('_tmp_ind', f.when(f.col('preg_start_date') < f.col('_delivery_date_lag1_p21d'), 1).otherwise(0))\
  .withColumn('_tmp_diff', f.when(f.col('_tmp_ind') == 1, f.datediff(f.col('preg_start_date'), f.col('_delivery_date_lag1_p21d'))).otherwise(0))

tmpt = tab(tmp1, '_rownum', '_tmp_ind', var2_unstyled=1); print()
tmpt = tab(tmp1, 'preg_start_date_estimated', '_tmp_ind', var2_unstyled=1); print()
tmpt = tabstat(tmp1.where(f.col('_tmp_ind') == 1), '_tmp_diff'); print()
tmpt = tab(tmp1, '_tmp_diff', '_tmp_ind', var2_unstyled=1); print()
tmpt = tab(tmp1, '_tmp_diff', 'preg_start_date_estimated', var2_unstyled=1); print()

# correct pregnancy start date and create indicator
vlist = vlist + ['preg_start_date', 'preg_start_date_estimated', 'preg_start_date_corrected']
tmp1 = tmp1\
  .withColumn('preg_start_date', f.when(f.col('_tmp_ind') == 1, f.col('_delivery_date_lag1_p21d')).otherwise(f.col('preg_start_date')))\
  .withColumnRenamed('_tmp_ind', 'preg_start_date_corrected')\
  .select(vlist)

# check
tmpt = tab(tmp1, 'preg_start_date_estimated', 'preg_start_date_corrected', var2_unstyled=1); print()

# COMMAND ----------

# ---------------------------------------------------------------------------------
# add fu date
# ---------------------------------------------------------------------------------
# NOTE
#   Censor for subsequent pregnancy start date for individuals with multiple delivery events
_win = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy('_rownum')

vlist = vlist + ['fu_end_date', 'fu_days']
tmp1 = tmp1\
  .withColumn('_delivery_date_p365', f.expr('date_add(delivery_date, 365)'))\
  .withColumn('_proj_fu_end_date', f.to_date(f.lit(proj_fu_end_date), 'yyyy-MM-dd'))\
  .withColumn('_preg_start_date_lagm1', f.lag(f.col('preg_start_date'), -1).over(_win))\
  .withColumn('fu_end_date',\
    f.least(
      '_delivery_date_p365'
      , '_proj_fu_end_date'
      , '_preg_start_date_lagm1' 
    )\
  )\
  .withColumn('fu_days', f.datediff(f.col('fu_end_date'), f.col('delivery_date')))\
  .select(vlist)

# check
tmpt = tabstat(tmp1, 'fu_days')

# cache
tmp1.cache()
print(f'{tmp1.count():,}')

# COMMAND ----------

# ---------------------------------------------------------------------------------
# check deaths
# ---------------------------------------------------------------------------------
# NOTE
#   Censor for death
tmp2 = tmp1\
  .withColumn('_dod_null', f.when((f.col('death_flag') == 1) & (f.col('DOD').isNull()), 1))\
  .withColumn('_dod_lt_delivery_date', f.when(f.col('DOD') < f.col('delivery_date'), 1))\
  .withColumn('_dod_lt_fu_end_date', f.when(f.col('DOD') < f.col('fu_end_date'), 1))

tmpt = tab(tmp2, '_dod_null', 'death_flag', var2_unstyled=1); print()
assert tmp2.select('_dod_null').where(f.col('_dod_null') == 1).count() == 0

tmpt = tab(tmp2, '_dod_lt_delivery_date', 'death_flag', var2_unstyled=1); print()
assert tmp2.select('_dod_lt_delivery_date').where(f.col('_dod_lt_delivery_date') == 1).count() == 2

tmpt = tab(tmp2, '_dod_lt_fu_end_date', 'death_flag', var2_unstyled=1); print()

count_var(tmp2, 'DOD'); print()

# ammend
tmp2 = tmp2\
  .withColumn('DOD', f.when(f.col('_dod_lt_delivery_date') == 1, None).otherwise(f.col('DOD')))\
  .drop('_dod_null', '_dod_lt_delivery_date', '_dod_lt_fu_end_date')

count_var(tmp2, 'DOD'); print()

# recalculate
tmp2 = tmp2\
  .withColumn('_delivery_date_p365', f.expr('date_add(delivery_date, 365)'))\
  .withColumn('_proj_fu_end_date', f.to_date(f.lit(proj_fu_end_date), 'yyyy-MM-dd'))\
  .withColumn('_preg_start_date_lagm1', f.lag(f.col('preg_start_date'), -1).over(_win))\
  .withColumn('fu_end_date',\
    f.least(
      '_delivery_date_p365'
      , '_proj_fu_end_date'
      , '_preg_start_date_lagm1' 
      , 'DOD' 
    )\
  )\
  .withColumn('fu_days', f.datediff(f.col('fu_end_date'), f.col('delivery_date')))\
  .drop('_delivery_date_p365', '_proj_fu_end_date', '_preg_start_date_lagm1')

# COMMAND ----------

old = tmp1
new = tmp2
file1, file2, file3, file3_differences = compare_files(old, new, ['PERSON_ID_DEID', '_rownum'], warningError=0)

# COMMAND ----------

display(file3_differences)

# COMMAND ----------

tmp1 = tmp2

# COMMAND ----------

display(tmp1)

# COMMAND ----------

# MAGIC %md # 6 Filter

# COMMAND ----------

# -----------------------------------------------------------------------------------
# apply delivery date cut-off
# -----------------------------------------------------------------------------------
# check
count_var(tmp1, 'PERSON_ID_DEID'); print()

print(proj_fu_end_date)
tmp1a = tmp1\
  .where(f.col('delivery_date') <= proj_fu_end_date)

# check
count_var(tmp1a, 'PERSON_ID_DEID'); print()
tmpt = tabstat(tmp1a, 'delivery_date', date=1); print()
tmpt = tabstat(tmp1a, 'fu_days'); print()


# -----------------------------------------------------------------------------------
# apply pregnancy start date project cut-offs
# -----------------------------------------------------------------------------------
print(proj_preg_start_date, proj_preg_end_date)
tmp2 = tmp1a\
  .where((f.col('preg_start_date') >= proj_preg_start_date) & (f.col('preg_start_date') <= proj_preg_end_date))

# check
count_var(tmp2, 'PERSON_ID_DEID'); print()
tmpt = tabstat(tmp2, 'preg_start_date', date=1); print()


# -----------------------------------------------------------------------------------
# anchor on gdppr 
# -----------------------------------------------------------------------------------
# check
count_var(_gdppr_id, 'PERSON_ID_DEID'); print()

tmp3 = merge(tmp2, _gdppr_id, ['PERSON_ID_DEID']); print()

# check
tmpt = tabstat(tmp3, var='delivery_date', byvar='_merge', date=1); print()

tmp3 = tmp3\
  .where(f.col('_merge') == 'both')\
  .drop('_merge')

# check
count_var(tmp3, 'PERSON_ID_DEID'); print()


# -----------------------------------------------------------------------------------
# filter to the first pregnancy only (after censoring for subsequent pregnancy above)
# -----------------------------------------------------------------------------------
# REcreate a row number variable to identify multiple records by ID
# NOTE
#   No longer need "'EPISTART', 'ADMIDATE', 'EPIEND', 'DISDATE', 'EPIORDER', 'EPIKEY'", since delivery_date is well-defined with intervals of >210 days

# define windows
_win_rownum = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy(['delivery_date'])
_win_rownummax = Window\
  .partitionBy('PERSON_ID_DEID')

# create _rownum and _rownummax
tmp3 = tmp3\
  .withColumn('_rownum', f.row_number().over(_win_rownum))\
  .withColumn('_rownummax', f.count('PERSON_ID_DEID').over(_win_rownummax))\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')\
  .withColumnRenamed('PERSON_ID_DEID_hes_apc_mat', 'PERSON_ID_hes_apc_mat')\
  .orderBy('PERSON_ID', '_rownum')

# check
tmpt = tab(tmp3.where(f.col('_rownum') == 1), '_rownummax'); print()
tmpt = tab(tmp3, '_rownum'); print()

# first delivery
tmp4 = tmp3\
  .where(f.col('_rownum') == 1)

# check
count_var(tmp4, 'PERSON_ID'); print()

# COMMAND ----------

# MAGIC %md # 8 Save

# COMMAND ----------

vlist_cohort = ['PERSON_ID', 'PERSON_ID_hes_apc_mat', '_id_agree', 'EPIKEY', 'preg_start_date', 'preg_start_date_estimated', 'preg_start_date_corrected', 'delivery_date', 'fu_end_date', 'fu_days']
vlist_delivery = [v for v in tmp4.columns if v not in vlist_cohort]
vlist_delivery.remove('_rownum')
vlist_delivery.remove('_rownummax')
vlist_delivery = ['PERSON_ID', 'PERSON_ID_hes_apc_mat', '_id_agree', 'EPIKEY'] + vlist_delivery
print(vlist_delivery)

# COMMAND ----------

# MAGIC %md ## 8.1 Multiple deliveries cohort

# COMMAND ----------

# saved for JN webinar plots (also produced below)
tmp3a = tmp3\
  .select(vlist_cohort + ['_rownum', '_rownummax'])

# COMMAND ----------

# check
display(tmp3a)

# COMMAND ----------

# check
tmpt = tab(tmp3a.where(f.col('_rownum') == 1), '_rownummax'); print()
tmpt = tabstat(tmp3a, var='delivery_date', byvar='_rownum', date=1); print()

# COMMAND ----------

# plot 
tmp3b = tmp3a\
  .withColumn('date_ym', f.date_format(f.col('preg_start_date'), 'yyyy-MM'))\
  .groupBy('date_ym', '_rownum')\
  .agg(\
    f.count(f.lit(1)).alias('n')\
  )\
  .orderBy('date_ym', f.desc('_rownum'))

display(tmp3b)



# COMMAND ----------

outName = f'{proj}_tmp_cohort'
spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
tmp3a.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md ## 8.2 First delivery cohort

# COMMAND ----------

tmp4a = tmp4\
  .select(vlist_cohort)

display(tmp4a)

# COMMAND ----------

# temporary step whilst developing - archive previous table before overwriting
outName = f'{proj}_out_cohort'.lower()
 

# save previous version for comparison purposes
_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
outName_pre = f'{outName}_pre{_datetimenow}'.lower()
print(outName_pre)
spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')


# COMMAND ----------

outName = f'{proj}_out_cohort'  
spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
tmp4a.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md ## 8.3 Delivery data

# COMMAND ----------

tmp4b = tmp4\
  .select(vlist_delivery)

display(tmp4b)


# COMMAND ----------

outName = f'{proj}_tmp_delivery_record'  
spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
tmp4b.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')



