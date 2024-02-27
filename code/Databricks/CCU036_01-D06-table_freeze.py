# Databricks notebook source
# MAGIC %md # CCU036_01-D06-table_freeze
# MAGIC  
# MAGIC **Description** This notebook creates the data snapshots restricted to the small cohort of interest.
# MAGIC  
# MAGIC **Author(s)** Adapted from the work of Tom Bolton (John Nolan, Elena Raffetti, CCU002)

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

datetimenow = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(datetimenow)

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

# spark.sql(f"""REFRESH TABLE {path_cohort}""")
cohort = spark.table(path_cohort)

display(cohort)

# COMMAND ----------

df_archive

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# check
count_var(cohort, 'PERSON_ID')

# cohort id
_cohort_id = cohort\
  .select('PERSON_ID')\
  .distinct()

# check
count_var(_cohort_id, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md # 3 Freeze tables

# COMMAND ----------

spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

for ind in df_archive.index:
  row = df_archive.iloc[ind]
  
  tabpath = row['database'] + '.' + row['table'] 
  prodDate = row['productionDate']
  id = row['idVar']
  print(f'{0 if ind<10 else ""}' + str(ind) + ' ' + tabpath + ' (' + prodDate + ')' + ' [' + id + ']', flush=True)
  
  # get archive table
  tmp = spark.table(tabpath)\
    .where(f.col('ProductionDate') == prodDate)  
  print(f'   {tmp.count():,}')
  
  # get cohort id with harmonised ID for join 
  tmpc = _cohort_id\
    .withColumnRenamed('PERSON_ID', id)  
  
  # join
  tmp = tmp\
    .join(tmpc, on=[id], how='inner')\
    .withColumn('freezeDate', f.lit(datetimenow))
  print(f'   {tmp.count():,} ({tmp.select(id).distinct().count():,})')
  
  tableName = row['table']
  
  # -----------------------------------------------------------------------------------------
  # temporary step whilst developing - archive previous table before overwriting
  # -----------------------------------------------------------------------------------------
  outName = f'{proj}_in_{tableName}'
  
  dbc_tables = spark.sql(f"""show tables in {dbc}""")
  tmpt = dbc_tables\
    .select('tableName')\
    .where(f.col('tableName') == outName)\
    .collect()
  if(len(tmpt)>0):
    spark.sql(f'ALTER TABLE {dbc}.{outName} RENAME TO {dbc}.{outName}_pre20230327')
    print('   renamed ' + outName + ' with suffix _pre20230327', flush=True)
  else:
    print('   NOT renamed ' + outName + ' (table not found)', flush=True)
  # -----------------------------------------------------------------------------------------
  
  # save  
  outName = f'{proj}_in_{tableName}'  
  spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
  tmp.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
  spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
  print('   saved ' + outName, flush=True)





