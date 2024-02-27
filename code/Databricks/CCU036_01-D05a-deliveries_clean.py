# Databricks notebook source
# NOTE
#   For simplicity, we have used _1 in the baby tail - ignoring _2 to _9 after exploratory analysis revealed that these 
#     higher indices did not provide additional evidence of delivery 


##Adapted from the work for ccu018_01


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

hes_apc_mat_del = spark.table(f'{dbc}.{proj}_tmp_hes_apc_mat_del')

# COMMAND ----------

# MAGIC %md # 2 Preparation

# COMMAND ----------

# check

count_var(hes_apc_mat_del, 'PERSON_ID_DEID')

# COMMAND ----------

# MAGIC %md ## 2.1 Reduce columns

# COMMAND ----------

tmp = hes_apc_mat_del

# drop columns
vlist_drop = [col for col in tmp.columns if re.match('^(BIRESUS|BIRORDR|BIRSTAT|BIRWEIT|DELMETH|DELPLAC|DELSTAT|GESTAT|SEXBABY)_[2-9]$', col)]
vlist_drop.extend(['PARTYEAR', 'ProductionDate_TB', 'FYEAR_hes_apc'])
vlist_drop.extend(['MATERNITY_EPISODE_TYPE', 'SEX', 'MYDOB', 'STARTAGE', 'STARTAGE_CALC']) 
# , 'NHSNOIND'
tmp = tmp\
  .drop(*vlist_drop)

# reorder columns
vlist_ordered = [
  'PERSON_ID_DEID'
  , 'PERSON_ID_DEID_hes_apc_mat'  
  , '_id_agree'    
  , 'EPIKEY'
  , 'FYEAR'
  , 'ADMIDATE'
  , 'EPISTART'
  , 'EPIEND'
  , 'DISDATE'
  , 'EPIORDER'
  , 'EPISTAT'
  , 'EPITYPE' 
]
# , 'SUSSPELLID'
vlist_all = list(tmp.columns)
vlist_unordered = [v for v in vlist_all if v not in vlist_ordered]
vlist_all_new = vlist_ordered + vlist_unordered
tmp1 = tmp\
  .select(*vlist_all_new)

display(tmp1)

# COMMAND ----------

# MAGIC %md ## 2.2 Create row total

# COMMAND ----------

# Create indicators for evidence of delivery for each maternity variable and create row total

# NOTE
#   Excluding NUMTAILB - default is 1, therefore not useful for providing evidence of delivery


tmp2 = tmp1\
  .withColumn('_ind_ANAGEST',   f.when(f.col('ANAGEST').isNotNull(), 1).otherwise(0))\
  .withColumn('_ind_ANASDATE',  f.when(f.col('ANASDATE').isNotNull(), 1).otherwise(0))\
  .withColumn('_ind_ANTEDUR',   f.when(f.col('ANTEDUR').isNotNull(), 1).otherwise(0))\
  .withColumn('_ind_BIRESUS_1', f.when(f.col('BIRESUS_1').isin(['1','2','3','4','5','6','8']), 1).otherwise(0))\
  .withColumn('_ind_BIRORDR_1', f.when(f.col('BIRORDR_1').isin(['1','2','3','4','5','6','7','8']), 1).otherwise(0))\
  .withColumn('_ind_BIRSTAT_1', f.when(f.col('BIRSTAT_1').isin(['1','2','3','4']), 1).otherwise(0))\
  .withColumn('_ind_BIRWEIT_1', f.when((f.col('BIRWEIT_1') >= 10) & (f.col('BIRWEIT_1') <= 7000), 1).otherwise(0))\
  .withColumn('_ind_DELCHANG',  f.when(f.col('DELCHANG').isin(['1','2','3','4','5','6','8']), 1).otherwise(0))\
  .withColumn('_ind_DELINTEN',  f.when(f.col('DELINTEN').isin(['0','1','2','3','4','5','6','7','8']), 1).otherwise(0))\
  .withColumn('_ind_DELMETH_1', f.when(f.col('DELMETH_1').isin(['0','1','2','3','4','5','6','7','8','9']), 1).otherwise(0))\
  .withColumn('_ind_DELMETH_D', f.when(f.col('DELMETH_D').isin(['01','02','03','04','05','06','07','08','09','10']), 1).otherwise(0))\
  .withColumn('_ind_DELONSET',  f.when(f.col('DELONSET').isin(['1','2','3','4','5','8']), 1).otherwise(0))\
  .withColumn('_ind_DELPLAC_1', f.when(f.col('DELPLAC_1').isin(['0','1','2','3','4','5','6','7','8']), 1).otherwise(0))\
  .withColumn('_ind_DELPOSAN',  f.when(f.col('DELPOSAN').isin(['0','1','2','3','4','5','6','7','8']), 1).otherwise(0))\
  .withColumn('_ind_DELPREAN',  f.when(f.col('DELPREAN').isin(['0','1','2','3','4','5','6','7','8']), 1).otherwise(0))\
  .withColumn('_ind_DELSTAT_1', f.when(f.col('DELSTAT_1').isin(['1','2','3','8']), 1).otherwise(0))\
  .withColumn('_ind_GESTAT_1',  f.when((f.col('GESTAT_1') >= 10) & (f.col('GESTAT_1') <= 49), 1).otherwise(0))\
  .withColumn('_ind_MATAGE',    f.when((f.col('MATAGE') >= 10) & (f.col('MATAGE') <= 70), 1).otherwise(0))\
  .withColumn('_ind_NUMBABY',   f.when(f.col('NUMBABY').isin(['1','2','3','4','5','6']), 1).otherwise(0))\
  .withColumn('_ind_NUMPREG',   f.when((f.col('NUMPREG') >= 0) & (f.col('NUMPREG') <= 19), 1).otherwise(0))\
  .withColumn('_ind_SEXBABY_1', f.when(f.col('SEXBABY_1').isin(['1','2']), 1).otherwise(0))\
  .withColumn('_ind_POSTDUR',   f.when(f.col('POSTDUR').isNotNull(), 1).otherwise(0))
tmp2 = tmp2\
  .withColumn('_rowtotal', sum([f.col(col) for col in tmp2.columns if re.match('^_ind_.*$', col)]))

# check
tmpt = tab(tmp2, '_rowtotal')

# COMMAND ----------

# MAGIC %md ## 2.3 Create indicators

# COMMAND ----------



tmp2 = tmp2\
  .withColumn('delivery_icd10',     f.when(f.col('DIAG_4_CONCAT').rlike('O8[0-4]|Z3(7|8)'), 1).otherwise(0))\
  .withColumn('delivery_opcs4',     f.when(f.col('OPERTN_4_CONCAT').rlike('P14[1-3]|R(1[4-9]|2[0-9]|3[0-2])'), 1).otherwise(0))\
  .withColumn('delivery_code',      f.greatest('delivery_icd10', 'delivery_opcs4'))\
  .withColumn('delivery_concat',    f.concat('delivery_icd10', 'delivery_opcs4'))\
  .withColumn('abomisc_lt21',       f.when(f.col('GESTAT_1') < 21, 1).otherwise(0))\
  .withColumn('abomisc_lt24_still', f.when((f.col('GESTAT_1') < 24) & (f.col('BIRSTAT_1').isin([2,3,4])), 1).otherwise(0))\
  .withColumn('abomisc_icd10',      f.when(f.col('DIAG_4_CONCAT').rlike('O0[0-8]'), 1).otherwise(0))\
  .withColumn('abomisc_opcs4',      f.when(f.col('OPERTN_4_CONCAT').rlike('Q10[12]|Q11[12356]|Q14|R03[1289]'), 1).otherwise(0))\
  .withColumn('abomisc_code',       f.greatest('abomisc_lt21', 'abomisc_lt24_still', 'abomisc_icd10', 'abomisc_opcs4'))\
  .withColumn('abomisc_concat',     f.concat('abomisc_lt21', 'abomisc_lt24_still', 'abomisc_icd10', 'abomisc_opcs4'))\
  .withColumn('care',               f.when(f.col('DIAG_4_CONCAT').rlike('O1[0-6]|O[234]|O6|O7[0-5]|O[89]|P|Z3[34569]'), 1).otherwise(0))\
  .withColumn('y951',               f.when(f.col('OPERTN_4_CONCAT').rlike('Y951'), 1).otherwise(0))\
  .withColumn('y952',               f.when(f.col('OPERTN_4_CONCAT').rlike('Y952'), 1).otherwise(0))\
  .withColumn('y953',               f.when(f.col('OPERTN_4_CONCAT').rlike('Y953'), 1).otherwise(0))\
  .withColumn('y954',               f.when(f.col('OPERTN_4_CONCAT').rlike('Y954'), 1).otherwise(0))\
  .withColumn('y95_concat',         f.concat('y951', 'y952', 'y953', 'y954'))\
  .withColumn('delivery_any',       f.when((f.col('delivery_code') == 1) | (f.col('abomisc_code') == 1) | (f.col('_rowtotal') > 0), 1).otherwise(0))

# check
tmpt = tab(tmp2, 'delivery_any')

# COMMAND ----------

# check
tmpt = tab(tmp2, 'delivery_icd10', 'delivery_opcs4', var2_unstyled=1); print()
tmpt = tab(tmp2, 'delivery_code'); print()
tmpt = tab(tmp2, 'delivery_code', 'abomisc_code', var2_unstyled=1); print()
tmpt = tab(tmp2, 'abomisc_concat', 'delivery_concat', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md ## 2.4 Create delivery date

# COMMAND ----------

# check ANTEDUR and POSTDUR
tmpt = tab(tmp2, 'ANTEDUR'); print()
tmpt = tab(tmp2, 'POSTDUR'); print()
tmpt = tab(tmp2, '_ind_ANTEDUR', '_ind_POSTDUR', var2_unstyled=1); print()

tmpp = tmp2\
  .withColumn('_tmp_ANTEDUR_POSTDUR', f.concat('_ind_ANTEDUR', '_ind_POSTDUR'))
tmpt = tab(tmpp, '_tmp_ANTEDUR_POSTDUR', 'FYEAR', var2_unstyled=1); print()
# Note: ANTEDUR and POSTDUR are not available for the latest financial year (2122)

tmpp = tmpp\
  .withColumn('delivery_date_ante', f.when(f.col('ANTEDUR').isNotNull(), f.expr('date_add(EPISTART, ANTEDUR)')))\
  .withColumn('delivery_date_post', f.when(f.col('POSTDUR').isNotNull(), f.expr('date_add(EPIEND, -POSTDUR)')))\
  .withColumn('standard_equality', f.col('delivery_date_ante') == f.col('delivery_date_post'))\
  .withColumn('null_safe_equality', udf_null_safe_equality('delivery_date_ante', 'delivery_date_post'))

# check
tmpt = tab(tmpp, 'standard_equality', 'null_safe_equality', var2_unstyled=1); print()

# => (ANTEDUR, EPISTART) and (POSTDUR, EPIEND) are consistent

# COMMAND ----------


tmp2 = tmp2\
  .withColumn('delivery_date_prov', f.greatest('_ind_ANTEDUR', '_ind_POSTDUR'))\
  .withColumn('delivery_date',\
    f.when(f.col('ANTEDUR').isNotNull(), f.expr('date_add(EPISTART, ANTEDUR)'))\
     .when(f.col('POSTDUR').isNotNull(), f.expr('date_add(EPIEND, -POSTDUR)'))\
     .otherwise(f.col('EPISTART'))\
  )

# check
tmpt = tab(tmp2, 'delivery_date_prov'); print()
tmpt = tabstat(tmp2, 'delivery_date', date=1)

# COMMAND ----------

# MAGIC %md # 3 Exclusion

# COMMAND ----------

# check
count_var(tmp2, 'PERSON_ID_DEID')

# COMMAND ----------

# MAGIC %md ## 3.1 Drop duplicate records

# COMMAND ----------

# drop
vlist = [col for col in tmp2.columns if col not in ['EPIKEY']]
# , 'SUSSPELLID'
tmp3 = tmp2\
  .dropDuplicates(vlist)

# check
count_var(tmp3, 'PERSON_ID_DEID')

# COMMAND ----------

# MAGIC %md ## 3.2 Drop records with no evidence of delivery

# COMMAND ----------

# check
tmpt = tab(tmp3, 'delivery_any'); print()

# drop
tmp4 = tmp3\
  .where(f.col('delivery_any') == 1)

# check
count_var(tmp4, 'PERSON_ID_DEID'); print()
tmpt = tab(tmp4, 'delivery_any'); print()

# COMMAND ----------

# MAGIC %md ## 3.3 Drop records failing quality assurance

# COMMAND ----------

tmp4.cache().count()

# COMMAND ----------

# TODO
#   Reproduce Stata scatterplots in this notebook

# check
tmpt = tab(tmp4, 'PROCODE3'); print()
tmpp = tmp4\
  .withColumn('_tmp', f.when(f.col('PROCODE3').isin(['RAE', 'RJ6']), 1).otherwise(0))
tmpt = tab(tmpp.where(f.col('_tmp') == 1), 'SITETRET'); print()
tmpt = tab(tmpp, 'abomisc_code', '_tmp', var2_unstyled=1); print()
tmpt = tab(tmpp, 'abomisc_concat', '_tmp', var2_unstyled=1); print()

# drop provider codes with severe departures from the expected GESTAT vs BIRWEIT relationship (see Stata scatterplots)
tmp5 = tmp4\
  .where(~f.col('PROCODE3').isin(['RAE', 'RJ6']))

# check
count_var(tmp5, 'PERSON_ID_DEID'); print()
tmpt = tab(tmp5, 'PROCODE3'); print()
tmpt = tab(tmp5, 'abomisc_concat', 'delivery_concat', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md # 4 Cleaning

# COMMAND ----------

tmp5.cache().count()

# COMMAND ----------

# create a row number variable to identify multiple records by ID
# note: add EPIKEY to orderBy to ensure results can be reproduced in the event of ties

# define windows
_win_rownum = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy(['delivery_date', 'EPISTART', 'ADMIDATE', 'EPIEND', 'DISDATE', 'EPIORDER', 'EPIKEY'])
_win_rownummax = Window\
  .partitionBy('PERSON_ID_DEID')

# create _rownum and _rownummax
tmp6 = tmp5\
  .withColumn('_rownum', f.row_number().over(_win_rownum))\
  .withColumn('_rownummax', f.count('PERSON_ID_DEID').over(_win_rownummax))\
  .orderBy('PERSON_ID_DEID', '_rownum')

# check
tmpt = tab(tmp6.where(f.col('_rownum') == 1), '_rownummax')

# COMMAND ----------

# MAGIC %md ## 4.1 Combine records within 28 days

# COMMAND ----------


vlist = ['delivery_date', 'EPISTART', 'ADMIDATE', 'EPIEND', 'DISDATE', 'EPIORDER', 'EPIKEY']
_win = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy(vlist)
_win_egen = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy(vlist)\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
_win_cumsum = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy(vlist)\
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
_win_28days = Window\
  .partitionBy('PERSON_ID_DEID', '_28days_grp')\
  .orderBy(f.desc('_rowtotal'), f.desc('delivery_date_prov'), f.desc('delivery_code'), f.desc('abomisc_code'), 'delivery_date', 'EPISTART', 'ADMIDATE', 'EPIEND', 'DISDATE', 'EPIORDER', 'EPIKEY')
_win_28days_rownummax = Window\
  .partitionBy('PERSON_ID_DEID', '_28days_grp')

# create date difference
# create minimum date difference
# create indicator for when the minimum date difference is >= 210 days (assumed to be the minimum legitimate interval between deliveries)
# ...
tmp6 = tmp6\
  .withColumn('_delivery_date_diff', f.datediff(f.col('delivery_date'), f.lag(f.col('delivery_date'), 1).over(_win)))\
  .withColumn('_delivery_date_diff_min', f.min(f.col('_delivery_date_diff')).over(_win_egen))\
  .withColumn('_delivery_date_diff_min_ge_210', f.when(f.col('_delivery_date_diff_min') >= 210, 1).otherwise(0))\
  .withColumn('_le28', f.when(f.col('_delivery_date_diff') <= 28, 1).otherwise(0))\
  .withColumn('_gt28_lt210', f.when((f.col('_delivery_date_diff') > 28) & (f.col('_delivery_date_diff') < 210), 1).otherwise(0))\
  .withColumn('_ge210', f.when(f.col('_delivery_date_diff') >= 210, 1).otherwise(0))\
  .withColumn('_le28_max', f.max(f.col('_le28')).over(_win_egen))\
  .withColumn('_gt28_lt210_max', f.max(f.col('_gt28_lt210')).over(_win_egen))\
  .withColumn('_ge210_max', f.max(f.col('_ge210')).over(_win_egen))\
  .withColumn('_concat', f.concat('_le28_max', '_gt28_lt210_max', '_ge210_max'))\
  .withColumn('_28days_newgrp', f.when(f.col('_delivery_date_diff') > 28, 1).otherwise(0))\
  .withColumn('_28days_grp', f.sum(f.col('_28days_newgrp')).over(_win_cumsum))\
  .withColumn('_28days_rownum', f.row_number().over(_win_28days))\
  .withColumn('_28days_rownummax', f.count('PERSON_ID_DEID').over(_win_28days_rownummax))\
  .withColumn('_28days_combine', f.when(f.col('_28days_rownummax') > 1, 1).otherwise(0))
  
# check
tmpt = tab(tmp6.where(f.col('_rownum') == 1), '_rownummax', '_delivery_date_diff_min_ge_210', var2_unstyled=1); print()

tmpp = tmp6\
  .where(\
    (f.col('_rownum') == 1)\
    & (f.col('_rownummax') > 1)\
    & (f.col('_delivery_date_diff_min_ge_210') == 0)\
  )
tmpt = tab(tmpp, '_concat'); print()

tmpt = tab(tmp6, '_28days_grp'); print()

tmpt = tab(tmp6.where(f.col('_28days_rownum') == 1), '_28days_rownummax'); print()

# COMMAND ----------

# MAGIC %md ### 4.1.1 Flag conflicts

# COMMAND ----------

# conflicts

# NOTE
#   For MATAGE, <None> did not work with isin, so, as a quick fix, used <99> as an artificial dummy value 
_dict = {
  'BIRWEIT_1': 9999,
  'GESTAT_1': 99,
  'MATAGE': 99,
  'NUMBABY': ['9', 'X'],
  'SEXBABY_1': ['0', '9']  
}

_win_conflict = Window\
  .partitionBy('PERSON_ID_DEID', '_28days_grp')\
  .orderBy('_28days_rownum')\
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
  # .rowsBetween(-sys.maxsize, 0))
_win_egen = Window\
  .partitionBy('PERSON_ID_DEID', '_28days_grp')\
  .orderBy('_28days_rownum')\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)            
            
for key in _dict:
  print(key, _dict[key])
  tmp6 = tmp6\
    .withColumn('_tmp', f.when(f.col('_28days_rownum') == 1, f.col(key)).otherwise(None))\
    .withColumn('_tmpm', f.last('_tmp', True).over(_win_conflict))\
    .withColumn('_tmpc', f.when((f.col(key).isNotNull()) & (~f.col(key).isin(_dict[key])) & (~udf_null_safe_equality(key, '_tmpm')), 1).otherwise(0))\
    .withColumn(f'_conflict_{key}', f.max(f.col('_tmpc')).over(_win_egen))

  tmpt = tab(tmp6, '_tmpc'); print()


# COMMAND ----------

# MAGIC %md ### 4.1.2 Combine

# COMMAND ----------

_win = Window\
  .partitionBy('PERSON_ID_DEID', '_28days_grp')\
  .orderBy('_28days_rownum')
_win_egen = Window\
  .partitionBy('PERSON_ID_DEID', '_28days_grp')\
  .orderBy('_28days_rownum')\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

varlist = ['EPIKEY', 'EPISTART', '_rowtotal', 'BIRWEIT_1', 'GESTAT_1', 'MATAGE', 'NUMBABY', 'SEXBABY_1', 'DIAG_4_CONCAT', 'OPERTN_4_CONCAT', 'delivery_date', 'delivery_date_prov', 'delivery_code', 'abomisc_code', 'care']

tmp7 = tmp6\
  .where(f.col('_28days_combine') == 1)
for var in varlist:
  print(var)
  tmp7 = tmp7\
    .withColumn(f'_28days_combine_{var}', f.flatten(f.collect_list(f.array(var)).over(_win)))\
    .withColumn(f'_28days_combine_{var}', f.max(f.col(f'_28days_combine_{var}')).over(_win_egen))
  
tmp7 = tmp7\
  .where(f.col('_28days_rownum') == 1)\
  .select(['PERSON_ID_DEID', '_28days_grp'] + [f'_28days_combine_{var}' for var in varlist])

print()
count_var(tmp6, 'PERSON_ID_DEID')
tmp6 = tmp6\
  .where(f.col('_28days_rownum') == 1)
count_var(tmp6, 'PERSON_ID_DEID')

print()
tmp8 = merge(tmp6, tmp7, ['PERSON_ID_DEID', '_28days_grp'])
assert tmp8.select('_merge').where(f.col('_merge') == 'right_only').count() == 0
tmp8 = tmp8\
  .drop('_merge')

# check
print()
count_var(tmp8, 'PERSON_ID_DEID')

# COMMAND ----------

# MAGIC %md ### 4.1.3 Recreate

# COMMAND ----------

# REcreate a row number variable to identify multiple records by ID
# note: add EPIKEY to orderBy to ensure results can be reproduced in the event of ties

# define windows
_win_rownum = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy(['delivery_date', 'EPISTART', 'ADMIDATE', 'EPIEND', 'DISDATE', 'EPIORDER', 'EPIKEY'])
_win_rownummax = Window\
  .partitionBy('PERSON_ID_DEID')

# create _rownum and _rownummax
tmp8 = tmp8\
  .withColumn('_rownum', f.row_number().over(_win_rownum))\
  .withColumn('_rownummax', f.count('PERSON_ID_DEID').over(_win_rownummax))\
  .orderBy('PERSON_ID_DEID', '_rownum')

# check
tmpt = tab(tmp8.where(f.col('_rownum') == 1), '_rownummax')

# COMMAND ----------

vlist = ['delivery_date', 'EPISTART', 'ADMIDATE', 'EPIEND', 'DISDATE', 'EPIORDER', 'EPIKEY']
_win = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy(vlist)
_win_egen = Window\
  .partitionBy('PERSON_ID_DEID')\
  .orderBy(vlist)\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# REcreate date difference within individuals
# REcreate minimum date difference per individual
# REcreate indicator for when the minimum date difference per individual is >= 210 days (assumed to be the minimum legitimate interval between deliveries)
tmp8 = tmp8\
  .withColumn('_delivery_date_diff', f.datediff(f.col('delivery_date'), f.lag(f.col('delivery_date'), 1).over(_win)))\
  .withColumn('_delivery_date_diff_min', f.min(f.col('_delivery_date_diff')).over(_win_egen))\
  .withColumn('_delivery_date_diff_min_ge_210', f.when(f.col('_delivery_date_diff_min') >= 210, 1).otherwise(0))

# check
tmpt = tab(tmp8.where(f.col('_rownum') == 1), '_rownummax', '_delivery_date_diff_min_ge_210', var2_unstyled=1)

# COMMAND ----------

# MAGIC %md # 5 Inclusion

# COMMAND ----------

# MAGIC %md ## 5.1 Keep records with an interval of at least 210 days

# COMMAND ----------

keep = tmp8\
  .where(\
    (f.col('_rownummax') == 1)\
    | (f.col('_delivery_date_diff_min_ge_210') == 1)\
  )
 
remaining = tmp8\
  .where(~(\
    (f.col('_rownummax') == 1)\
    | (f.col('_delivery_date_diff_min_ge_210') == 1)\
  ))

count_var(keep, 'PERSON_ID_DEID')
count_var(remaining, 'PERSON_ID_DEID')
print()
tmpt = tab(keep.where(f.col('_rownum') == 1), '_rownummax')
print()
tmpt = tab(remaining.where(f.col('_rownum') == 1), '_rownummax')

# COMMAND ----------

display(keep.where(f.col('_rownummax') == 4))

# COMMAND ----------

display(remaining)

# COMMAND ----------

# MAGIC %md # 6 Save

# COMMAND ----------

# select variables to keep and reorder
keep_select = keep\
  .select(\
     'PERSON_ID_DEID',
     'PERSON_ID_DEID_hes_apc_mat',
     '_id_agree',
     '_rownum',
     '_rownummax',
     'delivery_date_prov',
     'delivery_date',
     '_delivery_date_diff',
     '_delivery_date_diff_min',
     '_delivery_date_diff_min_ge_210',
     '_rowtotal',
     'delivery_code',
     'abomisc_code',
     'care',                            
     'DIAG_4_CONCAT',
     'OPERTN_4_CONCAT',          
  
     'BIRWEIT_1',                   
     'GESTAT_1',               
          
   
     'EPIKEY',
     'FYEAR',
     'PROCODE3',        
     'PROCODE5',         
     'SITETRET',         
     'ADMIDATE',
     'EPISTART',
     'EPIEND',
     'DISDATE',
     'EPIORDER',
     'EPISTAT',
     'EPITYPE',
          
     'ANAGEST',
     'ANASDATE',
     'ANTEDUR',
     'BIRESUS_1',
     'BIRORDR_1',
     'BIRSTAT_1',

     'DELCHANG',
     'DELINTEN',
     'DELMETH_1',
     'DELMETH_D',
     'DELONSET',
     'DELPLAC_1',
     'DELPOSAN',
     'DELPREAN',
     'DELSTAT_1',

     'MATAGE',
     'NUMBABY',
     'NUMPREG',
     'NUMTAILB',
     'POSTDUR',
     'SEXBABY_1',
     
     'delivery_icd10',
     'delivery_opcs4',     
     'delivery_concat',
          
     'abomisc_lt21',
     'abomisc_lt24_still',
     'abomisc_icd10',
     'abomisc_opcs4',
     'abomisc_concat',
     
     'y951',
     'y952',
     'y953',
     'y954',
     'y95_concat',
          
     'delivery_any',
          
     '_conflict_BIRWEIT_1',
     '_conflict_GESTAT_1',
     '_conflict_MATAGE',
     '_conflict_NUMBABY',
     '_conflict_SEXBABY_1',
          
     '_28days_combine_EPIKEY',
     '_28days_combine_EPISTART',
     '_28days_combine__rowtotal',
     '_28days_combine_BIRWEIT_1',
     '_28days_combine_GESTAT_1',
     '_28days_combine_MATAGE',
     '_28days_combine_NUMBABY',
     '_28days_combine_SEXBABY_1',
     '_28days_combine_DIAG_4_CONCAT',
     '_28days_combine_OPERTN_4_CONCAT',
     '_28days_combine_delivery_date',
     '_28days_combine_delivery_date_prov',
     '_28days_combine_delivery_code',
     '_28days_combine_abomisc_code',
     '_28days_combine_care'
  )
#   'SUSSPELLID',

display(keep_select)

# COMMAND ----------

# temporary step whilst developing - archive previous table before overwriting
outName = f'{proj}_tmp_hes_apc_mat_del_clean'.lower() # was _tmp_hes_apc_mat_clean

# save previous version for comparison purposes
_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
outName_pre = f'{outName}_pre{_datetimenow}'.lower()
print(outName_pre)
spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# COMMAND ----------

# save
keep_select.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md # 7 Compare

# COMMAND ----------

# temporary step whilst developing - compare previous version
outName = f'{proj}_tmp_hes_apc_mat_del_clean'.lower()
old = spark.table(f'{dbc}.{outName}_pre20230327_123119')
new = spark.table(f'{dbc}.{outName}')
file1, file2, file3, file3_differences = compare_files(old, new, ['EPIKEY'], warningError=0)



