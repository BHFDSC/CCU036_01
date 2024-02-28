# Databricks notebook source
# MAGIC %md # CCU036_01-D13-outcomes_post_pregnancy
# MAGIC
# MAGIC **Description** This notebook creates the outcomes post pregnancy.
# MAGIC
# MAGIC **Author(s)** Adapted from the work of Tom Bolton, John Nolan, & Elena Raffetti

# COMMAND ----------

# %md
# `CCU036_01_outcomes`:

# |Variable | Variable_Label|
# |----------------|--------------------|
# |PERSON_ID_DEID | Patient NHS Number |
# |record_date | Date of event: date in GDPPR, epistart HES |
# |name | Disease group |
# |term | Description of disease |
# |source | Data source (i.e., GDPPR, HES_APC, Deaths) |
# |terminology | code used | 
# |code | clinical code |

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

out_prefix = 'post_'

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

codelist     = spark.table(path_codelist)
cohort       = spark.table(path_cohort)
gdppr        = spark.table(path_gdppr)
hes_apc_long = spark.table(path_hes_apc_long)
deaths_long  = spark.table(path_deaths_long)

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates')
print('--------------------------------------------------------------------------------------')
print('individual_censor_dates'); print()
individual_censor_dates = cohort\
  .select('PERSON_ID', 'delivery_date', 'fu_end_date')\
  .withColumnRenamed('delivery_date', 'CENSOR_DATE_START')\
  .withColumnRenamed('fu_end_date', 'CENSOR_DATE_END')

# check
count_var(individual_censor_dates, 'PERSON_ID'); print()


print('--------------------------------------------------------------------------------------')
print('gdppr')
print('--------------------------------------------------------------------------------------')
_gdppr = gdppr\
  .select(['NHS_NUMBER_DEID', 'DATE', 'CODE'])\
  .withColumnRenamed('NHS_NUMBER_DEID', 'PERSON_ID')

# check
count_var(_gdppr, 'PERSON_ID'); print()

# add censor dates
_gdppr = _gdppr\
  .join(individual_censor_dates, on='PERSON_ID', how='inner')

# check
count_var(_gdppr, 'PERSON_ID'); print()

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
_gdppr = _gdppr\
  .where(\
    (f.col('DATE') > f.col('CENSOR_DATE_START'))\
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))\
  )

# check
count_var(_gdppr, 'PERSON_ID'); print()


print('--------------------------------------------------------------------------------------')
print('hes_apc')
print('--------------------------------------------------------------------------------------')
# filter to primary diagnosis position
_hes_apc = hes_apc_long\
  .where(f.col('DIAG_POSITION') == 1)\
  .select(['PERSON_ID', 'EPISTART', 'CODE', 'DIAG_POSITION'])\
  .withColumnRenamed('EPISTART', 'DATE')

# check
count_var(_hes_apc, 'PERSON_ID'); print()
tmpt = tab(_hes_apc, 'DIAG_POSITION'); print()

# add censor dates
_hes_apc = _hes_apc\
  .drop('DIAG_POSITION')\
  .join(individual_censor_dates, on='PERSON_ID', how='inner')

# check
count_var(_hes_apc, 'PERSON_ID'); print()

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
_hes_apc = _hes_apc\
  .where(\
    (f.col('DATE') > f.col('CENSOR_DATE_START'))\
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))\
  )

# check
count_var(_hes_apc, 'PERSON_ID'); print()


print('--------------------------------------------------------------------------------------')
print('deaths')
print('--------------------------------------------------------------------------------------')
# filter to underlying diagnosis position
_deaths = deaths_long\
  .where(f.col('DIAG_POSITION') == 'UNDERLYING')\
  .select(['PERSON_ID', 'DATE', 'CODE', 'DIAG_POSITION'])

# check
count_var(_deaths, 'PERSON_ID'); print()
tmpt = tab(_deaths, 'DIAG_POSITION'); print()

# add censor dates
_deaths = _deaths\
  .drop('DIAG_POSITION')\
  .join(individual_censor_dates, on='PERSON_ID', how='inner')

# check
count_var(_deaths, 'PERSON_ID'); print()

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
_deaths = _deaths\
  .where(\
    (f.col('DATE') > f.col('CENSOR_DATE_START'))\
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))\
  )

# check
count_var(_deaths, 'PERSON_ID'); print()


print('--------------------------------------------------------------------------------------')
print('cache')
print('--------------------------------------------------------------------------------------')
_gdppr.cache()
print(f'_gdppr   {_gdppr.count():,}')
_hes_apc.cache()
print(f'_hes_apc {_hes_apc.count():,}')
_deaths.cache()
print(f'_deaths  {_deaths.count():,}')

# COMMAND ----------

# MAGIC %md # 3 Outcomes

# COMMAND ----------

# MAGIC %md ## 3.1 Codelist

# COMMAND ----------

# check covariate only outcomes
codelist_out_covar_only = codelist\
  .where(\
    (f.col('name').isin([\
      'AMI'
      , 'DIC'                                                       
      , 'DVT'
      , 'DVT_other'  
      , 'DVT_pregnancy'                                                       
      , 'HF'
      , 'ICVT'
      , 'ICVT_pregnancy'      
      , 'PE'                                                       
      , 'PVT'   
      , 'TTP'                                                          
      , 'angina'
      , 'angina_unstable'    
      , 'artery_dissect'                                                       
      , 'cardiomyopathy'
      , 'life_arrhythmias'
      , 'mesenteric_thrombus'                                                       
      , 'myocarditis'    
      , 'other_arterial_embolism'                                                       
      , 'pericarditis'
      , 'stroke_HS'
      , 'stroke_IS'
      , 'stroke_NOS'      
      , 'stroke_SAH' 
      , 'stroke_TIA' 
      , 'thrombocytopenia'
      , 'thrombophilia'  
    ]))\
    & ((f.col('covariate_only') == 1) | (f.col('code_type') != 1))\
  )

# check
tmpt = tab(codelist_out_covar_only, 'name' , 'terminology', var2_unstyled=1); print()

# COMMAND ----------

display(codelist_out_covar_only)
# note. follows CCU002_01 - all SNOMED codes for ICVT, PE, stroke_HS, thrombocytopenia

# COMMAND ----------

# check
tmpt = tab(codelist, 'name' , 'terminology', var2_unstyled=1); print()

# outcomes codelist
codelist_out = codelist\
  .where(\
    (f.col('name').isin([\
      'AMI'
      , 'DIC'                                                       
      , 'DVT'
      , 'DVT_other'  
      , 'DVT_pregnancy'                                                       
      , 'HF'
      , 'ICVT'
      , 'ICVT_pregnancy'      
      , 'PE'                                                       
      , 'PVT'   
      , 'TTP'                                                          
      , 'angina'
      , 'angina_unstable'    
      , 'artery_dissect'                                                       
      , 'cardiomyopathy'
      , 'life_arrhythmias'
      , 'mesenteric_thrombus'                                                       
      , 'myocarditis'    
      , 'other_arterial_embolism'                                                       
      , 'pericarditis'
      , 'stroke_HS'
      , 'stroke_IS'
      , 'stroke_NOS'      
      , 'stroke_SAH' 
      , 'stroke_TIA' 
      , 'thrombocytopenia'
      , 'thrombophilia'  
    ]))\
    & ((f.col('covariate_only').isNull()) | (f.col('covariate_only') != 1))\
    & ((f.col('code_type').isNull()) | (f.col('code_type') == '') | (f.col('code_type') == 1))\
  )

# check
tmpt = tab(codelist_out, 'name' , 'terminology', var2_unstyled=1); print()

# snomed
codelist_out_snomed = codelist_out\
  .where(\
    (f.col('terminology') == 'SNOMED')\
    & (f.col('name').isin([\
      'AMI'
      , 'HF'
      , 'angina'
      , 'angina_unstable'
      , 'stroke_IS'
      , 'stroke_NOS'
      , 'stroke_TIA'
    ]))\
  )

# check
tmpt = tab(codelist_out_snomed, 'name' , 'terminology', var2_unstyled=1); print()

# icd10
codelist_out_icd10 = codelist_out\
  .where(f.col('terminology') == 'ICD10')

# check
tmpt = tab(codelist_out_icd10, 'name' , 'terminology', var2_unstyled=1); print()

# COMMAND ----------

# composite events (to be defined as columns within the codelist going forward)
codelist_out_composite = {}
for term in ['snomed', 'icd10']:
  print(term)
  _tmpm = []
  for i, c in enumerate(composite_events):
    print(' ', i, c, '=', composite_events[c])
    _tmp = globals()[f'codelist_out_{term}']\
      .where(f.col('name').isin(composite_events[c]))\
      .withColumnRenamed('name', 'name_old')\
      .withColumn('name', f.lit(c))     
    if(i == 0): _tmpm = _tmp
    else: _tmpm = _tmpm.unionByName(_tmp)
  tmpt = tab(_tmpm, 'name_old', 'name', var2_unstyled=1); print()  
  codelist_out_composite[term] = _tmpm

# snomed
codelist_out_snomed = codelist_out_snomed\
  .withColumn('name_old', f.lit(''))\
  .unionByName(codelist_out_composite['snomed'])
 
# check
tmpt = tab(codelist_out_snomed, 'name' , 'terminology', var2_unstyled=1); print()  
  
# icd10
codelist_out_icd10 = codelist_out_icd10\
  .withColumn('name_old', f.lit(''))\
  .unionByName(codelist_out_composite['icd10'])

# check
tmpt = tab(codelist_out_icd10, 'name' , 'terminology', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md ## 3.2 Create

# COMMAND ----------

_out_post_in = {
    'gdppr':   ['_gdppr',   'codelist_out_snomed', 2]
  , 'hes_apc': ['_hes_apc', 'codelist_out_icd10',  1]
  , 'deaths':  ['_deaths',  'codelist_out_icd10',  3]
}
_out_post, _out_post_1st, _out_post_1st_wide = codelist_match(_out_post_in, _name_prefix=f'out_{out_prefix}')
_out_post_summ_name, _out_post_summ_name_code = codelist_match_summ(_out_post_in, _out_post)

# COMMAND ----------

# MAGIC %md ## 3.3 Check

# COMMAND ----------

display(_out_post_1st_wide)

# COMMAND ----------

_tmp = _out_post_1st\
  .withColumn('diff', f.datediff(f.col('DATE'), f.col('CENSOR_DATE_START'))/365.25)
_tmpp = _tmp\
  .toPandas()

rows_of_5 = np.ceil(len(_tmpp['name'].drop_duplicates())/5).astype(int)
fig, axes = plt.subplots(rows_of_5, 5, figsize=(13,2*rows_of_5), sharex=True) # , sharey=True , dpi=100) # 
 
colors = sns.color_palette("tab10", 3)
names = ['hes_apc', 'gdppr', 'deaths']  
  
vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
  print(i, ax, v)
  tmp2d1 = _tmpp[(_tmpp[f'diff'] > -30) & (_tmpp[f'name'] == v)]
  s1 = list(tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'diff'])
  s2 = list(tmp2d1[tmp2d1[f'source'] == 'gdppr'][f'diff'])
  s3 = list(tmp2d1[tmp2d1[f'source'] == 'deaths'][f'diff'])
  ax.hist([s1, s2, s3], bins = list(np.linspace(0,1,100)), stacked=True, color=colors, label=names) # normed=True
  ax.set_title(f'{v}')
  ax.xaxis.set_tick_params(labelbottom=True)
  if(i==0): ax.legend(loc='upper right')
plt.tight_layout();
display(fig)

# COMMAND ----------

_tmp1 = _tmp\
  .withColumn('byvar', f.concat_ws('_', 'source', 'name'))
tmpt = tabstat(_tmp1, 'diff', byvar='byvar')

# COMMAND ----------

_tmp = _out_post_1st\
  .withColumn('diff', f.datediff(f.col('DATE'), f.col('CENSOR_DATE_START'))/365.25)
_tmpp = _tmp\
  .toPandas()

rows_of_5 = np.ceil(len(_tmpp['name'].drop_duplicates())/5).astype(int)
fig, axes = plt.subplots(rows_of_5, 5, figsize=(13,2*rows_of_5), sharex=True, sharey=True) #  , dpi=100) # 
 
colors = sns.color_palette("tab10", 3)
names = ['hes_apc', 'gdppr', 'deaths']  
  
vlist = list(_tmpp[['name']].drop_duplicates().sort_values('name')['name']) # ['AMI', 'BMI_obesity', 'CKD', 'COPD']  
for i, (ax, v) in enumerate(zip(axes.flatten(), vlist)):
  print(i, ax, v)
  tmp2d1 = _tmpp[(_tmpp[f'diff'] > -30) & (_tmpp[f'name'] == v)]
  s1 = list(tmp2d1[tmp2d1[f'source'] == 'hes_apc'][f'diff'])
  s2 = list(tmp2d1[tmp2d1[f'source'] == 'gdppr'][f'diff'])
  s3 = list(tmp2d1[tmp2d1[f'source'] == 'deaths'][f'diff'])
  ax.hist([s1, s2, s3], bins = list(np.linspace(0,1,100)), stacked=True, color=colors, label=names) # normed=True
  ax.set_title(f'{v}')
  ax.xaxis.set_tick_params(labelbottom=True)
  if(i==0): ax.legend(loc='upper right')
plt.tight_layout();
display(fig)

# COMMAND ----------
# COMMAND ----------

display(_out_post_summ_name)

# COMMAND ----------

display(_out_post_summ_name_code)

# COMMAND ----------

# MAGIC %md # 4 Save

# COMMAND ----------



tmp1 = _out_post_1st_wide

count_var(tmp1, 'PERSON_ID'); print()  

# add cohort_id
_cohort_id = cohort\
  .select('PERSON_ID')

# check
count_var(_cohort_id, 'PERSON_ID'); print()

# merge
tmp2 = merge(tmp1, _cohort_id, ['PERSON_ID']); print()
tmp2 = tmp2\
  .drop('_merge')

# check
count_var(tmp2, 'PERSON_ID'); print()

# COMMAND ----------

# temporary step whilst developing - archive previous table before overwriting
outName = f'{proj}_out_outcomes_{out_prefix}preg'.lower()
 

# save previous version for comparison purposes
_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
outName_pre = f'{outName}_pre{_datetimenow}'.lower()
print(outName_pre)
spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# COMMAND ----------

outName = f'{proj}_out_outcomes_{out_prefix}preg'.lower()
spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
tmp2.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

list_tables = []
list_tables = list_tables + ['_out_post_summ_name', '_out_post_summ_name_code']
for i, table in enumerate(list_tables):
  print(i, table)
  outName = f'{proj}_out_codelist_match{table}'.lower()
  tmp1 = globals()[table]
  tmp1.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
  spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
  print(f'  saved {dbc}.{outName}')

# COMMAND ----------

# MAGIC %md # 5 Compare

# COMMAND ----------

# temporary step whilst developing - compare previous version
outName = f'{proj}_out_outcomes_{out_prefix}preg'.lower()
old = spark.table(f'{dbc}.{outName}_pre20230329_101022')
new = spark.table(f'{dbc}.{outName}')
file1, file2, file3, file3_differences = compare_files(old, new, ['PERSON_ID'], warningError=0)

# COMMAND ----------



# COMMAND ----------

# temporary step whilst developing - compare previous version
outName = f'{proj}_out_outcomes_{out_prefix}preg'.lower()
old = spark.table(f'{dbc}.{outName}_pre20220527')
for v in [col for col in old.columns if re.match(f'^out_post_', col)]:
  print(v)
  old = old\
    .withColumnRenamed(v, v + '_date')

new = spark.table(f'{dbc}.{outName}')
file1, file2, file3, file3_differences = compare_files(old, new, ['PERSON_ID'], warningError=0)

# COMMAND ----------



# COMMAND ----------

# temporary step whilst developing - compare previous version
outName = f'{proj}_out_outcomes_{out_prefix}preg'.lower()
old = spark.table(f'{dbc}.{outName}_pre20220527')
new = spark.table(f'{dbc}.{outName}')
file1, file2, file3, file3_differences = compare_files(old, new, ['PERSON_ID'], warningError=0)

# COMMAND ----------



# COMMAND ----------

# temporary step whilst developing - compare previous version
outName = f'{proj}_out_outcomes_{out_prefix}preg'.lower()
old = spark.table(f'{dbc}.{outName}_pre20220517')
new = spark.table(f'{dbc}.{outName}')
file1, file2, file3, file3_differences = compare_files(old, new, ['PERSON_ID'], warningError=0)

# COMMAND ----------

display(file3_differences)

# COMMAND ----------

# checks
# tmpt = tab(new, 'out_post_xxx'); print()

tmpt = new
for v in [col for col in list(new.columns) if re.match(f'^out_{out_prefix}', col)]:
  tmpt = tmpt\
    .withColumn(v + '_flag', f.when(f.col(v).isNotNull(), 1).otherwise(0)) 
for i, c in enumerate(composite_events):
  print(' ', i, c, '=', composite_events[c]); print()
  tmpt = tmpt\
    .withColumn('tmpchk_' + c, sum([f.col(f'out_{out_prefix}' + v.lower() + '_flag') for v in composite_events[c]]))\
    .withColumn('tmpchk_' + c, f.when(f.col('tmpchk_' + c) > 0, 1).otherwise(0))\
    .withColumn('tmpchk_' + c + '_null_equality', udf_null_safe_equality(f'out_{out_prefix}' + c + '_flag', 'tmpchk_' + c))
  tmpr = tab(tmpt, f'out_{out_prefix}' + c + '_flag', 'tmpchk_' + c, var2_unstyled=1); print()
  assert tmpt.select('tmpchk_' + c + '_null_equality').where(f.col('tmpchk_' + c + '_null_equality') == False).count() == 0

# COMMAND ----------

