# Databricks notebook source
# MAGIC %md # CCU036_01-D07-curated_tables
# MAGIC
# MAGIC **Description** This notebook produces the curated tables.
# MAGIC
# MAGIC **Author(s)** Adapted from the work of Tom Bolton for cc018 (John Nolan, Elena Raffetti, CCU002)

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

hes_apc = spark.table(path_hes_apc)
deaths = spark.table(path_deaths)
geog = spark.table(path_geog)
hes_op = spark.table(path_hes_op)

# COMMAND ----------

# MAGIC %md # 2 HES_APC

# COMMAND ----------

# check
count_var(hes_apc, 'PERSON_ID_DEID')
count_var(hes_apc, 'EPIKEY')

# COMMAND ----------

# MAGIC %md ## 2.1 Diag

# COMMAND ----------

# MAGIC %md ### 2.1.1 Create

# COMMAND ----------

_hes_apc = hes_apc\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')\
  .select(['PERSON_ID', 'EPIKEY', 'EPISTART'] + [col for col in list(hes_apc.columns) if re.match(r'^DIAG_(3|4)_\d\d$', col)])\
  .orderBy('PERSON_ID', 'EPIKEY')  

# reshape twice, tidy, and remove records with missing code
hes_apc_long = reshape_wide_to_long_multi(_hes_apc, i=['PERSON_ID', 'EPIKEY', 'EPISTART'], j='POSITION', stubnames=['DIAG_4_', 'DIAG_3_'])
hes_apc_long = reshape_wide_to_long_multi(hes_apc_long, i=['PERSON_ID', 'EPIKEY', 'EPISTART', 'POSITION'], j='DIAG_DIGITS', stubnames=['DIAG_'])\
  .withColumnRenamed('POSITION', 'DIAG_POSITION')\
  .withColumn('DIAG_POSITION', f.regexp_replace('DIAG_POSITION', r'^[0]', ''))\
  .withColumn('DIAG_DIGITS', f.regexp_replace('DIAG_DIGITS', r'[_]', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'X$', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'[.,\-\s]', ''))\
  .withColumnRenamed('DIAG_', 'CODE')\
  .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))\
  .orderBy(['PERSON_ID', 'EPIKEY', 'DIAG_DIGITS', 'DIAG_POSITION'])

# COMMAND ----------

# MPR possibly remove
hes_op = spark.table(path_hes_op)
count_var(hes_op, 'PERSON_ID_DEID')
count_var(hes_op, 'ATTENDKEY')

_hes_op = hes_op\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')\
  .select(['PERSON_ID','ATTENDKEY', 'APPTDATE'] + [col for col in list(hes_op.columns) if re.match(r'^DIAG_(3|4)_\d\d$', col)])\
  .orderBy('PERSON_ID', 'ATTENDKEY')  

# reshape twice, tidy, and remove records with missing code
hes_op_long = reshape_wide_to_long_multi(_hes_op, i=['PERSON_ID', 'ATTENDKEY', 'APPTDATE'], j='POSITION', stubnames=['DIAG_4_', 'DIAG_3_'])
hes_op_long = reshape_wide_to_long_multi(hes_op_long, i=['PERSON_ID', 'ATTENDKEY', 'APPTDATE', 'POSITION'], j='DIAG_DIGITS', stubnames=['DIAG_'])\
  .withColumnRenamed('POSITION', 'DIAG_POSITION')\
  .withColumn('DIAG_POSITION', f.regexp_replace('DIAG_POSITION', r'^[0]', ''))\
  .withColumn('DIAG_DIGITS', f.regexp_replace('DIAG_DIGITS', r'[_]', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'X$', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'[.,\-\s]', ''))\
  .withColumnRenamed('DIAG_', 'CODE')\
  .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))\
  .orderBy(['PERSON_ID', 'ATTENDKEY', 'DIAG_DIGITS', 'DIAG_POSITION'])

# COMMAND ----------

display(hes_op_long)

# COMMAND ----------

# MAGIC %md ### 2.1.2 Check

# COMMAND ----------

# check
count_var(hes_apc_long, 'PERSON_ID'); print()
count_var(hes_apc_long, 'EPIKEY'); print()

# check removal of trailing X
tmpt = hes_apc_long\
  .where(f.col('CODE').rlike('X'))\
  .withColumn('flag', f.when(f.col('CODE').rlike('^X.*'), 1).otherwise(0))
tmpt = tab(tmpt, 'flag'); print()

# COMMAND ----------

display(hes_apc_long)

# COMMAND ----------

# MAGIC %md ### 2.1.3 Save

# COMMAND ----------

# temporary step whilst developing - archive previous table before overwriting
outName = f'{proj}_in_hes_apc_all_years_archive_long'.lower()
 

# save previous version for comparison purposes
_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
outName_pre = f'{outName}_pre{_datetimenow}'.lower()
print(outName_pre)
spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')



# COMMAND ----------

outName = f'{proj}_in_hes_apc_all_years_archive_long'.lower()  
spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
hes_apc_long.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md ## 2.2 Oper

# COMMAND ----------

# MAGIC %md ### 2.2.1 Create

# COMMAND ----------

oper_out = hes_apc\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')\
  .select(['PERSON_ID', 'EPIKEY', 'EPISTART'] + [col for col in list(hes_apc.columns) if re.match(r'^OPERTN_(3|4)_\d\d$', col)])\
  .orderBy('PERSON_ID', 'EPIKEY')
  
# reshape twice, tidy, and remove records with missing code
oper_out_long = reshape_wide_to_long_multi(oper_out, i=['PERSON_ID', 'EPIKEY', 'EPISTART'], j='DIAG_POSITION', stubnames=['OPERTN_4_', 'OPERTN_3_'])
oper_out_long = reshape_wide_to_long_multi(oper_out_long, i=['PERSON_ID', 'EPIKEY', 'EPISTART', 'DIAG_POSITION'], j='DIAG_DIGITS', stubnames=['OPERTN_'])\
  .withColumn('DIAG_POSITION', f.regexp_replace('DIAG_POSITION', r'^[0]', ''))\
  .withColumn('DIAG_DIGITS', f.regexp_replace('DIAG_DIGITS', r'[_]', ''))\
  .withColumn('OPERTN_', f.regexp_replace('OPERTN_', r'[.,\-\s]', ''))\
  .withColumnRenamed('OPERTN_', 'CODE')\
  .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))

# COMMAND ----------

# MAGIC %md ### 2.2.2 Check

# COMMAND ----------

# check
count_var(oper_out_long, 'PERSON_ID'); print()
count_var(oper_out_long, 'EPIKEY'); print()

tmpt = tab(oper_out_long, 'DIAG_DIGITS'); print()
tmpt = tab(oper_out_long, 'DIAG_POSITION'); print() 
tmpt = tab(oper_out_long, 'CODE'); print() 
# TODO - add valid OPCS-4 code checker...

# COMMAND ----------

display(oper_out_long)

# COMMAND ----------

# MAGIC %md ### 2.2.3 Save

# COMMAND ----------

# temporary step whilst developing - archive previous table before overwriting
outName = f'{proj}_in_hes_apc_all_years_archive_oper_long'.lower()
 

# save previous version for comparison purposes
_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
outName_pre = f'{outName}_pre{_datetimenow}'.lower()
print(outName_pre)
spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')



# COMMAND ----------

outName = f'{proj}_in_hes_apc_all_years_archive_oper_long'.lower()  
spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
oper_out_long.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md # 3 Deaths

# COMMAND ----------

# MAGIC %md ## 3.1 Create

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
deaths_out = deaths\
  .withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'PERSON_ID')\
  .where(f.col('PERSON_ID').isNotNull())\
  .withColumn('REG_DATE', f.to_date(f.col('REG_DATE'), 'yyyyMMdd'))\
  .withColumn('REG_DATE_OF_DEATH', f.to_date(f.col('REG_DATE_OF_DEATH'), 'yyyyMMdd'))\
  .withColumn('_rownum', f.row_number().over(_win))\
  .where(f.col('_rownum') == 1)\
  .select(['PERSON_ID', 'REG_DATE', 'REG_DATE_OF_DEATH', 'S_UNDERLYING_COD_ICD10'] + [col for col in list(deaths.columns) if re.match(r'^S_COD_CODE_\d(\d)*$', col)])\
  .withColumnRenamed('S_UNDERLYING_COD_ICD10', 'S_COD_CODE_UNDERLYING')\
  .orderBy('PERSON_ID')

# check
count_var(deaths_out, 'PERSON_ID')
count_var(deaths_out, 'REG_DATE_OF_DEATH')
count_var(deaths_out, 'S_COD_CODE_UNDERLYING')

# single row deaths 
deaths_out_sing = deaths_out

# remove records with missing DOD
deaths_out = deaths_out\
  .where(f.col('REG_DATE_OF_DEATH').isNotNull())\
  .drop('REG_DATE')

# check
count_var(deaths_out, 'PERSON_ID')

# reshape
# add 1 to diagnosis position to start at 1 (c.f., 0) - will avoid confusion with HES long, which start at 1
# rename 
# remove records with missing cause of death
deaths_out_long = reshape_wide_to_long(deaths_out, i=['PERSON_ID', 'REG_DATE_OF_DEATH'], j='DIAG_POSITION', stubname='S_COD_CODE_')\
  .withColumn('DIAG_POSITION', f.when(f.col('DIAG_POSITION') != 'UNDERLYING', f.concat(f.lit('SECONDARY_'), f.col('DIAG_POSITION'))).otherwise(f.col('DIAG_POSITION')))\
  .withColumnRenamed('S_COD_CODE_', 'CODE4')\
  .where(f.col('CODE4').isNotNull())\
  .withColumnRenamed('REG_DATE_OF_DEATH', 'DATE')\
  .withColumn('CODE3', f.substring(f.col('CODE4'), 1, 3))
deaths_out_long = reshape_wide_to_long(deaths_out_long, i=['PERSON_ID', 'DATE', 'DIAG_POSITION'], j='DIAG_DIGITS', stubname='CODE')\
  .withColumn('CODE', f.regexp_replace('CODE', r'[.,\-\s]', ''))
  
# check
count_var(deaths_out_long, 'PERSON_ID')  
tmpt = tab(deaths_out_long, 'DIAG_POSITION', 'DIAG_DIGITS', var2_unstyled=1) 
tmpt = tab(deaths_out_long, 'CODE')  
# TODO - add valid ICD-10 code checker...

# COMMAND ----------

# MAGIC %md ## 3.2 Check

# COMMAND ----------

display(deaths_out_sing)

# COMMAND ----------

display(deaths_out_long)

# COMMAND ----------

# MAGIC %md ## 3.3 Save

# COMMAND ----------

# temporary step whilst developing - archive previous table before overwriting
outName = f'{proj}_in_deaths_{db}_archive_sing'.lower()
 

# save previous version for comparison purposes
_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
outName_pre = f'{outName}_pre{_datetimenow}'.lower()
print(outName_pre)
spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')



# COMMAND ----------

# temporary step whilst developing - archive previous table before overwriting
outName = f'{proj}_in_deaths_{db}_archive_long'.lower()
 

# save previous version for comparison purposes
_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
outName_pre = f'{outName}_pre{_datetimenow}'.lower()
print(outName_pre)
spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')


# COMMAND ----------

outName = f'{proj}_in_deaths_{db}_archive_sing'.lower()
spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
deaths_out_sing.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

outName = f'{proj}_in_deaths_{db}_archive_long'.lower()
spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
deaths_out_long.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md # 4 LSOA region lookup

# COMMAND ----------

# MAGIC %md ## 4.1 Create

# COMMAND ----------

spark.sql(f"""
  CREATE or replace global temporary view {proj}_lsoa_region_lookup AS
  with curren_chd_geo_listings as (
    SELECT * 
    FROM {path_geog}
    --WHERE IS_CURRENT = 1
  ),
  lsoa_auth as (
    SELECT e01.geography_code as lsoa_code, e01.geography_name lsoa_name, 
      e02.geography_code as msoa_code, e02.geography_name as msoa_name, 
      e0789.geography_code as authority_code, e0789.geography_name as authority_name,
      e0789.parent_geography_code as authority_parent_geography
    FROM curren_chd_geo_listings e01
    LEFT JOIN curren_chd_geo_listings e02 on e02.geography_code = e01.parent_geography_code
    LEFT JOIN curren_chd_geo_listings e0789 on e0789.geography_code = e02.parent_geography_code
    WHERE e01.geography_code like 'E01%' and e02.geography_code like 'E02%'
  ),
  auth_county as (
    SELECT lsoa_code, lsoa_name,
           msoa_code, msoa_name,
           authority_code, authority_name,
           e10.geography_code as county_code, e10.geography_name as county_name,
           e10.parent_geography_code as parent_geography
    FROM lsoa_auth
    LEFT JOIN dss_corporate.ons_chd_geo_listings e10 on e10.geography_code = lsoa_auth.authority_parent_geography
    WHERE LEFT(authority_parent_geography,3) = 'E10'
  ),
  auth_met_county as (
    SELECT lsoa_code, lsoa_name,
           msoa_code, msoa_name,
           authority_code, authority_name,
           NULL as county_code, NULL as county_name,           
           lsoa_auth.authority_parent_geography as region_code
    FROM lsoa_auth
    WHERE LEFT(authority_parent_geography,3) = 'E12'
  ),
  lsoa_region_code as (
    SELECT lsoa_code, lsoa_name,
           msoa_code, msoa_name,
           authority_code, authority_name,
           county_code, county_name, 
           auth_county.parent_geography as region_code
    FROM auth_county
    UNION ALL
    SELECT lsoa_code, lsoa_name,
           msoa_code, msoa_name,
           authority_code, authority_name,
           county_code, county_name, 
           region_code 
    FROM auth_met_county
  ),
  lsoa_region as (
    SELECT lsoa_code, lsoa_name,
           msoa_code, msoa_name,
           authority_code, authority_name,
           county_code, county_name, 
           region_code, e12.geography_name as region_name 
    FROM lsoa_region_code
    LEFT JOIN dss_corporate.ons_chd_geo_listings e12 on lsoa_region_code.region_code = e12.geography_code
  )
  SELECT * FROM lsoa_region
""")

# COMMAND ----------

tmp1 = spark.table(f'global_temp.{proj}_lsoa_region_lookup')

# COMMAND ----------

# MAGIC %md ## 4.2 Check

# COMMAND ----------

display(tmp1)

# COMMAND ----------

count_var(tmp1, 'lsoa_code')

# COMMAND ----------

# check duplicates
w1 = Window\
  .partitionBy('lsoa_code')\
  .orderBy('lsoa_name')
w2 = Window\
  .partitionBy('lsoa_code')
tmp2 = tmp1\
  .withColumn('_rownum', f.row_number().over(w1))\
  .withColumn('_rownummax', f.count('lsoa_code').over(w2))\
  .where(f.col('_rownummax') > 1)
display(tmp2)
# duplicates are a result of an authority name change - not relevant for this project

# COMMAND ----------

tmp2 = tmp1\
  .withColumn('_rownum', f.row_number().over(w1))\
  .where(f.col('_rownum') == 1)\
  .select('lsoa_code', 'lsoa_name', 'region_code', 'region_name')

count_var(tmp2, 'lsoa_code')

# COMMAND ----------

tmpt = tab(tmp2, 'region_name')

# COMMAND ----------

# MAGIC %md ## 4.3 Save

# COMMAND ----------

# temporary step whilst developing - archive previous table before overwriting
outName = f'{proj}_in_lsoa_region_lookup'.lower()
 

# save previous version for comparison purposes
_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
outName_pre = f'{outName}_pre{_datetimenow}'.lower()
print(outName_pre)
spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# outName = f'{proj}_out_cohort'.lower()

# COMMAND ----------

outName = f'{proj}_in_lsoa_region_lookup'.lower()
spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
tmp2.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md # 5 HES_OP

# COMMAND ----------

# check
count_var(hes_op, 'PERSON_ID_DEID')
count_var(hes_op, 'ATTENDKEY')


# COMMAND ----------

# MAGIC %md ## 5.1 Diag

# COMMAND ----------

# MAGIC %md ### 5.1.1 Create

# COMMAND ----------

_hes_op = hes_op\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')\
  .select(['PERSON_ID','ATTENDKEY', 'APPTDATE'] + [col for col in list(hes_op.columns) if re.match(r'^DIAG_(3|4)_\d\d$', col)])\
  .orderBy('PERSON_ID', 'ATTENDKEY')  

# reshape twice, tidy, and remove records with missing code
hes_op_long = reshape_wide_to_long_multi(_hes_op, i=['PERSON_ID', 'ATTENDKEY', 'APPTDATE'], j='POSITION', stubnames=['DIAG_4_', 'DIAG_3_'])
hes_op_long = reshape_wide_to_long_multi(hes_op_long, i=['PERSON_ID', 'ATTENDKEY', 'APPTDATE', 'POSITION'], j='DIAG_DIGITS', stubnames=['DIAG_'])\
  .withColumnRenamed('POSITION', 'DIAG_POSITION')\
  .withColumn('DIAG_POSITION', f.regexp_replace('DIAG_POSITION', r'^[0]', ''))\
  .withColumn('DIAG_DIGITS', f.regexp_replace('DIAG_DIGITS', r'[_]', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'X$', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'[.,\-\s]', ''))\
  .withColumnRenamed('DIAG_', 'CODE')\
  .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))\
  .orderBy(['PERSON_ID', 'ATTENDKEY', 'DIAG_DIGITS', 'DIAG_POSITION'])

# COMMAND ----------

# MAGIC %md ### 5.1.2 Check

# COMMAND ----------

display(hes_op)

# COMMAND ----------

# check
count_var(hes_op_long, 'PERSON_ID'); 
count_var(hes_op_long, 'ATTENDKEY'); 

# check removal of trailing X
tmpt = hes_op_long\
  .where(f.col('CODE').rlike('X'))\
  .withColumn('flag', f.when(f.col('CODE').rlike('^X.*'), 1).otherwise(0))
tmpt = tab(tmpt, 'flag'); print()

# COMMAND ----------

display(hes_op_long)

# COMMAND ----------

# MAGIC %md ### 5.1.3 Save

# COMMAND ----------

# temporary step whilst developing - archive previous table before overwriting
outName = f'{proj}_in_hes_op_all_years_archive_long'.lower()
 

# save previous version for comparison purposes
_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
outName_pre = f'{outName}_pre{_datetimenow}'.lower()
print(outName_pre)
spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')



# COMMAND ----------

outName = f'{proj}_in_hes_op_all_years_archive_long'.lower()  
spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
hes_op_long.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')