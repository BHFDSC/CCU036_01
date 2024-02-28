# Databricks notebook source
# MAGIC %md
# MAGIC # CCU036_01-D11b_exposure
# MAGIC Description This notebook creates the exposure table.
# MAGIC
# MAGIC Author :Arun 
# MAGIC
# MAGIC Input data:
# MAGIC
# MAGIC dars_nic_391419_j3w9t_collab.ccu036_01_vaccination
# MAGIC
# MAGIC dars_nic_391419_j3w9t_collab.ccu036_01_out_cohort
# MAGIC
# MAGIC
# MAGIC Output table:
# MAGIC
# MAGIC dars_nic_391419_j3w9t_collab.ccu036_01_out_exposure

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

# DBTITLE 1,Parameters
# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU036_01/CCU036_01-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

cohort   = spark.table(path_cohort)
vaccine  = spark.table(path_vaccine)


# COMMAND ----------

count_var(cohort, 'PERSON_ID'); print()
count_var(vaccine, 'NHS_NUMBER_DEID'); print()

# COMMAND ----------

# MAGIC %md # 2 Prepare data

# COMMAND ----------

vlist= ['PERSON_ID', 'preg_start_date', 'delivery_date']
tmp1 = cohort\
  .select(vlist); 
 
_vaccine = vaccine\
  .withColumnRenamed('NHS_NUMBER_DEID', 'PERSON_ID')
tmp1 = merge(tmp1, _vaccine, ['PERSON_ID']); print()
tmp1 = tmp1\
  .where(f.col('_merge').isin(['left_only', 'both']))\
  .drop('_merge')
 
# check
count_var(tmp1, 'PERSON_ID'); print()
 


# COMMAND ----------

display(tmp1)

# COMMAND ----------

# MAGIC %md # 3. Vaccine products check

# COMMAND ----------

tab1= tab(tmp1, 'vaccination_dose1_product');print()
tab2= tab(tmp1, 'vaccination_dose2_product');print()
tab3= tab(tmp1, 'vaccination_dose3_product');print()
tab4= tab(tmp1, 'vaccination_dose_booster_product');print()
tab5= tab(tmp1, 'vaccination_conflicted');print()

# COMMAND ----------


tab6=tab(tmp1, 'vaccination_dose1_product', 'vaccination_dose2_product', var2_unstyled=1); print()
tab7=tab(tmp1, 'vaccination_dose2_product', 'vaccination_dose3_product', var2_unstyled=1); print()
tab8=tab(tmp1, 'vaccination_dose2_product', 'vaccination_dose_booster_product', var2_unstyled=1); print()


# COMMAND ----------

count_var(tmp1, 'vaccination_dose1_date'); print()
count_var(tmp1, 'vaccination_dose1_product'); print()
count_var(tmp1, 'vaccination_dose2_date'); print()
count_var(tmp1, 'vaccination_dose2_product'); print()
count_var(tmp1, 'vaccination_dose3_date'); print()
count_var(tmp1, 'vaccination_dose3_product'); print()
count_var(tmp1, 'vaccination_dose_booster_date'); print()
count_var(tmp1, 'vaccination_dose_booster_product'); print()


# COMMAND ----------

# MAGIC %md # 4 Start date of vaccine course

# COMMAND ----------

from pyspark.sql.functions import least
tmp2 = tmp1\
  .withColumn('min_date',least('vaccination_dose1_date', 'vaccination_dose2_date', 'vaccination_dose3_date', 'vaccination_dose_booster_date'))

count_var(tmp2, 'min_date'); print()

# COMMAND ----------

tab(tmp2, 'min_date');print()

# COMMAND ----------

# MAGIC %md # 5 Vaccine exposure status

# COMMAND ----------


tmp3= tmp2\
  .withColumn('boost', 
              f.when(((f.col('vaccination_dose1_product')=='AstraZeneca') & (f.col('vaccination_dose2_product')=='AstraZeneca') & (f.col('vaccination_dose_booster_product')=='Pfizer')),1).\
              otherwise(f.when(((f.col('vaccination_dose1_product')=='AstraZeneca') & (f.col('vaccination_dose2_product')=='AstraZeneca') & (f.col('vaccination_dose_booster_product')=='Moderna')),1).\
                        otherwise(f.when(((f.col('vaccination_dose1_product')=='Pfizer') & (f.col('vaccination_dose2_product')=='Pfizer') & (f.col('vaccination_dose_booster_product')=='Pfizer')),1).\
                                  otherwise(f.when(((f.col('vaccination_dose1_product')=='Pfizer') & (f.col('vaccination_dose2_product')=='Pfizer') & (f.col('vaccination_dose_booster_product')=='Moderna')),1).\
                                            otherwise(f.when(((f.col('vaccination_dose1_product')=='Moderna') & (f.col('vaccination_dose2_product')=='Moderna') & (f.col('vaccination_dose_booster_product')=='Pfizer')),1).\
                                                      otherwise(f.when(((f.col('vaccination_dose1_product')=='Moderna') & (f.col('vaccination_dose2_product')=='Moderna') & (f.col('vaccination_dose_booster_product')=='Moderna')),1)))))))\
    .withColumn('second', 
              f.when(((f.col('vaccination_dose1_product')=='AstraZeneca') & (f.col('vaccination_dose2_product')=='AstraZeneca') ),1).\
              otherwise(f.when(((f.col('vaccination_dose1_product')=='Pfizer') & (f.col('vaccination_dose2_product')=='Pfizer')),1).\
                        otherwise(f.when(((f.col('vaccination_dose1_product')=='Moderna') & (f.col('vaccination_dose2_product')=='Moderna') ),1))))\
    .withColumn('first', 
              f.when(((f.col('vaccination_dose1_product')=='AstraZeneca') ),1).\
              otherwise(f.when(((f.col('vaccination_dose1_product')=='Pfizer') ),1).\
                        otherwise(f.when(((f.col('vaccination_dose1_product')=='Moderna')  ),1))))\
    .withColumn('exposure_vaccine', 
              f.when(((f.col('boost')==1) & (f.col('vaccination_dose_booster_date')<f.col('preg_start_date')) ),3).\
                otherwise(f.when(((f.col('second')==1) & (f.col('vaccination_dose2_date')<f.col('preg_start_date')) ),2).\
                         otherwise(f.when(((f.col('first')==1) & (f.col('vaccination_dose1_date')<f.col('preg_start_date')) ),1).\
                                  otherwise(f.when(((f.col('min_date').isNull()) | (f.col('min_date')>f.col('delivery_date')) ),0).\
                                           otherwise(4)))))
   

tab1= tab(tmp3, 'boost');print()
tab2= tab(tmp3, 'second');print()
tab3= tab(tmp3, 'first');print()
tab4= tab(tmp3, 'exposure_vaccine');print()
#tab2=tab(tmp3, 'vaccination_dose1_product', 'exposure'); print()

# COMMAND ----------

tab7=tab(tmp3, 'vaccination_dose1_product', 'exposure_vaccine', var2_unstyled=1); print()


# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %md # 6 Save

# COMMAND ----------

outName = f'{proj}_out_exposure'.lower()
 

# save previous version for comparison purposes
_datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
outName_pre = f'{outName}_pre{_datetimenow}'.lower()
print(outName_pre)
spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# COMMAND ----------

outName = f'{proj}_out_exposure'.lower()
spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
tmp3.write.saveAsTable(f'{dbc}.{outName}')
spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md # 7 Compare

# COMMAND ----------

# temporary step whilst developing - compare previous version
# outName = f'{proj}_out_exposure'.lower()
old = spark.table(f'{dbc}.{outName_pre}')
new = spark.table(f'{dbc}.{outName}')
file3_differences = compare_files(old, new, ['PERSON_ID'], warningError=0)


# COMMAND ----------

# MAGIC %md #8 Check

# COMMAND ----------

tmp4 = tmp3\
    .where(f.col('exposure_vaccine')==4)

tmp4= tmp4\
  .withColumn('d1_dt',
              f.when(((f.col('vaccination_dose1_date').isNull()) & (f.col('first')==1)),1 ))\
  .withColumn('d2_dt', 
             f.when(((f.col('vaccination_dose2_date').isNull()) & (f.col('second')==1)),1))\
  .withColumn('db_dt',
             f.when(((f.col('vaccination_dose_booster_date').isNull()) & (f.col('boost')==1)),1).otherwise(0))\
  .withColumn('dur_preg',
             f.when(((f.col('vaccination_dose1_date')>=f.col('preg_start_date')) & (f.col('first')==1)),1).\
             otherwise(f.when(((f.col('vaccination_dose2_date')>=f.col('preg_start_date')) & (f.col('second')==1)),2).\
                      otherwise(f.when(((f.col('vaccination_dose_booster_date')>=f.col('preg_start_date')) & (f.col('boost')==1)),3).\
                                otherwise(f.when((f.col('vaccination_dose1_product').isNull())|(f.col('vaccination_dose2_product').isNull())|(f.col('vaccination_dose_booster_product').isNull()),4).\
                                otherwise(f.when((f.col('vaccination_dose1_product')!=(f.col('vaccination_dose2_product'))),5).\
                                         otherwise(f.when(f.col('min_date').isNull(),0)))))))
                               

tab1= tab(tmp4, 'd1_dt');print()
tab2= tab(tmp4, 'd2_dt');print()
tab3= tab(tmp4, 'db_dt');print()
tab4= tab(tmp4, 'd1_dt','d2_dt');print()
tab5= tab(tmp4, 'dur_preg');print()

# COMMAND ----------

tab1=tab(tmp4, 'first');print()
tab2=tab(tmp4,'second');print()
tab3=tab(tmp4,'boost');print()

# COMMAND ----------

tmp5= tmp4\
  .where(f.col('dur_preg').isNull())

display(tmp5)