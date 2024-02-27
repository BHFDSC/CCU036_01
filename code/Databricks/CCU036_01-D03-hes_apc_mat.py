# Databricks notebook source
# MAGIC %md # CCU036_01-D03-hes_apc_mat
# MAGIC
# MAGIC **Description** This notebook produces a hes_apc_mat table that also includes data from hes_apc
# MAGIC
# MAGIC **Author(s)** Adapted from the work of Tom Bolton for ccu018_01

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

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

# get archive tables at the pre-specified ProductionDate
hes_apc     = get_archive_table('hes_apc')
hes_apc_mat = get_archive_table('hes_apc_mat')
gdppr       = get_archive_table('gdppr')

# COMMAND ----------

# check FYEAR
tmpt = tab(hes_apc, 'FYEAR'); print()
tmpt = tab(hes_apc_mat, 'FYEAR'); print()

# COMMAND ----------

# filter to relevant FYEAR
_list_fyears = ['1920', '2021', '2122']
_hes_apc = hes_apc\
  .where(f.col('FYEAR').isin(_list_fyears))
_hes_apc_mat = hes_apc_mat\
  .where(f.col('FYEAR').isin(_list_fyears))

# COMMAND ----------

# check FYEAR
tmpt = tab(_hes_apc, 'FYEAR'); print()
tmpt = tab(_hes_apc_mat, 'FYEAR'); print()

# COMMAND ----------

# reduce _hes_apc
vlist = ['FYEAR'] 
vlist.extend(['EPIKEY', 'PERSON_ID_DEID']) 
vlist.extend(['EPISTART', 'ADMIDATE', 'EPIEND', 'DISDATE'])
vlist.extend(['EPIORDER', 'EPISTAT', 'EPITYPE'])
vlist.extend(['SEX', 'MYDOB', 'STARTAGE', 'STARTAGE_CALC'])
vlist.extend(['DIAG_4_CONCAT', 'OPERTN_4_CONCAT'])
vlist.extend(['PROCODE3', 'PROCODE5', 'SITETRET'])

_hes_apc = _hes_apc\
  .select(vlist)\
  .withColumnRenamed('FYEAR', 'FYEAR_hes_apc')\
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID_DEID_hes_apc')

# COMMAND ----------

# gdppr id
_gdppr_id = gdppr\
  .select('NHS_NUMBER_DEID')\
  .distinct()

# check
count_var(_gdppr_id, 'NHS_NUMBER_DEID')

# COMMAND ----------

# MAGIC %md # 2 Merge

# COMMAND ----------

# check for missing and duplicate keys before merge
count_var(_hes_apc, 'EPIKEY')
count_var(_hes_apc_mat, 'EPIKEY')

# COMMAND ----------

# check for common vars before merge
tmp1 = pd.DataFrame({'varname':_hes_apc.columns})
tmp2 = pd.DataFrame({'varname':_hes_apc_mat.columns})
tmpm = pd.merge(tmp1, tmp2, on='varname', how='outer', validate="one_to_one", indicator=True)
vlist = tmpm[tmpm['_merge'] == 'both']['varname'].tolist()
assert vlist == ['EPIKEY']

# COMMAND ----------

# merge
tmp1 = merge(_hes_apc_mat, _hes_apc, ['EPIKEY']); print()
assert tmp1.count() == tmp1.where(f.col('_merge').isin(['both', 'right_only'])).count() 

# keep 'both' only
tmp2 = tmp1\
  .where(f.col('_merge') == 'both')\
  .drop('_merge')

# check fyear agreement
tmpt = tab(tmp2, 'FYEAR', 'FYEAR_hes_apc', var2_unstyled=1); print()

# check id agreement
tmp2 = tmp2\
  .withColumn('_id_agree', f.when(f.col('PERSON_ID_DEID') == f.col('PERSON_ID_DEID_hes_apc'), 1).otherwise(0))
tmpt = tab(tmp2, '_id_agree', 'FYEAR', var2_unstyled=1); print()

# COMMAND ----------

tmpt = tmp2\
  .where(f.col('_id_agree') == 0)
display(tmpt)


# COMMAND ----------

# MAGIC %md # 3 Save



# COMMAND ----------


outName = f'{proj}_in_hes_apc_mat'.lower()
 

# save previous version for comparison purposes
_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
outName_pre = f'{outName}_pre{_datetimenow}'.lower()
print(outName_pre)
spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# COMMAND ----------

outName = f'{proj}_in_gdppr_id'.lower()
 

# save previous version for comparison purposes
_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
outName_pre = f'{outName}_pre{_datetimenow}'.lower()
print(outName_pre)
spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')



# COMMAND ----------

outName = f'{proj}_in_hes_apc_mat'  
spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
tmp2.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

outName = f'{proj}_in_gdppr_id'  
spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
_gdppr_id.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')


