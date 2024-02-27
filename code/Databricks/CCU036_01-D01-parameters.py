# Databricks notebook source
# %md
# **Description** This notebook defines a set of parameters for use across CCU036_01
 
# **Project(s)** CCU036_01
 
# **Author(s)** Arun and Rachel Denhom adapted from Thomas Bolton

# **Approach**

# - This notebook is loaded when each notebook in the analysis pipleine is run, so helper functions and parameters are consistently available



# COMMAND ----------

# MAGIC
# MAGIC %run "/Shared/SHDS/common/functions"

# COMMAND ----------

# %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/functions"

# COMMAND ----------

import pyspark.sql.functions as f
import pandas as pd
import re

# COMMAND ----------

# -----------------------------------------------------------------------------
# Project
# -----------------------------------------------------------------------------
proj = 'ccu036_01'


# -----------------------------------------------------------------------------
# Databases
# -----------------------------------------------------------------------------
db = 'dars_nic_391419_j3w9t'
dbc = f'{db}_collab'
dbc_new='dsa_391419_j3w9t_collab'
db_new = 'dsa_391419_j3w9t'


# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
# maternity files (to be incorporated into archive files when the archives are available - freezing for now)
path_hes_apc_mat_1920 = f'{db}.hes_apc_mat_1920_{db}'
path_hes_apc_mat_2021 = f'{db}.hes_apc_mat_2021_{db}'
path_hes_apc_mat_2122 = f'{db}.hes_apc_mat_2122_{db}'

path_hes_apc_otr_1920 = f'{db}.hes_apc_otr_1920_{db}'
path_hes_apc_otr_2021 = f'{db}.hes_apc_otr_2021_{db}'
path_hes_apc_otr_2122 = f'{db}.hes_apc_otr_2122_{db}'

path_hes_apc_1920 = f'{db}.hes_apc_1920_{db}'
path_hes_apc_2021 = f'{db}.hes_apc_2021_{db}'
path_hes_apc_2122 = f'{db}.hes_apc_2122_{db}'
<<<<<<< Updated upstream
tmp_archive_date = '2023-02-28 00:00:00.000000'

=======
tmp_archive_date = '2022-03-31 00:00:00.000000'
tmp_archived_on =tmp_archive_date 
>>>>>>> Stashed changes
# archive tables
data = [
    ['deaths',  dbc, f'deaths_{db}_archive',            tmp_archive_date, 'DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'REG_DATE_OF_DEATH']
  , ['gdppr',   dbc, f'gdppr_{db}_archive',             tmp_archive_date, 'NHS_NUMBER_DEID',                'DATE']
  , ['hes_apc', dbc, f'hes_apc_all_years_archive',      '2022-12-31 00:00:00', 'PERSON_ID_DEID',                 'EPISTART'] 
  , ['hes_apc_mat', dbc, f'hes_apc_mat_all_years_archive',  '2022-12-31 00:00:00', 'PERSON_ID_DEID',                 'EPISTART'] 
  , ['hes_op',  dbc, f'hes_op_all_years_archive',       '2022-12-31 00:00:00', 'PERSON_ID_DEID',                 'APPTDATE'] 
  , ['hes_ae',  dbc, f'hes_ae_all_years_archive',       '2022-12-31 00:00:00', 'PERSON_ID_DEID',                 'ARRIVALDATE'] 
  , ['pmeds',   dbc, f'primary_care_meds_{db}_archive', '2023-01-31 15:42:15.787335', 'Person_ID_DEID',                 'ProcessingPeriodDate']
  , ['hes_cc',  dbc, f'hes_cc_all_years_archive',       '2022-12-31 00:00:00', 'PERSON_ID_DEID',                 'CCSTARTDATE']
  , ['chess',   dbc, f'chess_{db}_archive',             tmp_archive_date, 'PERSON_ID_DEID',                 'InfectionSwabDate']
  , ['sgss',    dbc, f'sgss_{db}_archive',              tmp_archive_date, 'PERSON_ID_DEID',                 'Specimen_Date']
  , ['sus',     dbc, f'sus_{db}_archive',               '2022-09-30 15:35:02.550882', 'NHS_NUMBER_DEID',                'EPISODE_START_DATE']
  , ['vacc',        dbc, f'vaccine_status_{db}_archive',    tmp_archive_date, 'PERSON_ID_DEID',                 'RECORDED_DATE']
]
df_archive = pd.DataFrame(data, columns = ['dataset', 'database', 'table', 'productionDate', 'idVar', 'dateVar'])
df_archive
  # check isid dataset and table

# project in tables (available post table_freeze)
path_hes_apc_mat     = f'{dbc}.{proj}_in_hes_apc_mat'
path_gdppr_id        = f'{dbc}.{proj}_in_gdppr_id'
path_deaths          = f'{dbc}.{proj}_in_deaths_{db}_archive'
path_gdppr           = f'{dbc}.{proj}_in_gdppr_{db}_archive'
path_hes_apc         = f'{dbc}.{proj}_in_hes_apc_all_years_archive'
path_hes_op          = f'{dbc}.{proj}_in_hes_op_all_years_archive'
path_hes_ae          = f'{dbc}.{proj}_in_hes_ae_all_years_archive'
path_pmeds           = f'{dbc}.{proj}_in_primary_care_meds_{db}_archive'
path_hes_cc          = f'{dbc}.{proj}_in_hes_cc_all_years_archive'
path_chess           = f'{dbc}.{proj}_in_chess_{db}_archive'
path_sgss            = f'{dbc}.{proj}_in_sgss_{db}_archive'
path_sus             = f'{dbc}.{proj}_in_sus_{db}_archive'
path_vacc            = f'{dbc}.{proj}_in_vaccine_status_{db}_archive'
path_skinny          = f'{dbc}.{proj}_out_skinny'

# project in curated tables
path_hes_apc_long      = f'{dbc}.{proj}_in_hes_apc_all_years_archive_long'
path_hes_apc_oper_long = f'{dbc}.{proj}_in_hes_apc_all_years_archive_oper_long'
path_hes_op_long       = f'{dbc}.{proj}_in_hes_op_all_years_archive_long'
path_deaths_long       = f'{dbc}.{proj}_in_deaths_{db}_archive_long'
path_deaths_sing       = f'{dbc}.{proj}_in_deaths_{db}_archive_sing'
path_lsoa_region       = f'{dbc}.{proj}_in_lsoa_region_lookup'
path_covid             = f'{dbc}.{proj}_in_covid'

# project temporary tables
path_delivery_record    = f'{dbc}.{proj}_tmp_delivery_record'
path_vaccine           = f'{dbc}.{proj}_vaccination'

# project out tables
path_codelist          = f'{dbc}.{proj}_out_codelist'
path_cohort            = f'{dbc}.{proj}_out_cohort'
path_exposure          = f'{dbc}.{proj}_out_exposure'


# reference files
path_bhf_phenotypes  = 'bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127'
path_map_ctv3_snomed = 'dss_corporate.read_codes_map_ctv3_to_snomed'
path_gdppr_snomed    = 'dss_corporate.gpdata_snomed_refset_full'
path_icd10           = 'dss_corporate.icd10_group_chapter_v01'
path_geog            = 'dss_corporate.ons_chd_geo_listings'
path_imd             = 'dss_corporate.english_indices_of_dep_v02'
path_ethnic_hes      = 'dss_corporate.hesf_ethnicity'
path_ethnic_gdppr    = 'dss_corporate.gdppr_ethnicity'


# -----------------------------------------------------------------------------
# Dates
# -----------------------------------------------------------------------------
proj_preg_start_date = '2020-12-08' 
proj_preg_end_date   = '2021-12-31' #43 weeks prior to last available HES/GDPPR 
proj_fu_end_date     = '2022-10-28' 

# -----------------------------------------------------------------------------
# Composite events
# -----------------------------------------------------------------------------
# Note: to be defined as columns within the codelist going forward
composite_events = {
  "arterial": ['AMI', 'stroke_IS', 'stroke_NOS', 'other_arterial_embolism'],
  "venous": ['PE', 'PVT', 'DVT', 'DVT_other', 'DVT_pregnancy', 'ICVT', 'ICVT_pregnancy'],
  "haematological": ['DIC', 'TTP', 'thrombocytopenia'],
  "other_cvd": ['stroke_SAH', 'stroke_HS', 'mesenteric_thrombus', 'artery_dissect', 'life_arrhythmias', 'cardiomyopathy', 'HF', 'angina', 'angina_unstable']
}


# COMMAND ----------

# function to get archive table at the specified productionDate
def get_archive_table(dataset):
  row = df_archive[df_archive['dataset'] == dataset]
  assert row.shape[0] == 1
  row = row.iloc[0]
  path = row['database'] + '.' + row['table']  
  productionDate = row['productionDate']  
  print(path + ' (' + productionDate + ')')
  tmp = spark.table(path)\
    .where(f.col('ProductionDate') == productionDate)  
  print(f'  {tmp.count():,}')
  # idVar = row['idVar']
  # count_var(tmp, idVar)
  return tmp

 

# COMMAND ----------

print(f'Project:')
print("  {0:<22}".format('proj') + " = " + f'{proj}') 
print(f'')
print(f'Databases:')
print("  {0:<22}".format('db') + " = " + f'{db}') 
print("  {0:<22}".format('dbc') + " = " + f'{dbc}') 
print("  {0:<22}".format('db_new') + " = " + f'{db_new}') 
print("  {0:<22}".format('dbc_new') + " = " + f'{dbc_new}') 
print(f'')
print(f'Paths:')
# with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
#  print(df_paths_raw_data[['dataset', 'database', 'table']])
print(f'')
print(f'  df_archive')
print(df_archive[['dataset', 'database', 'table', 'productionDate']].to_string())
print(f'')
tmp = vars().copy()
for var in list(tmp.keys()):
  if(re.match('^path_.*$', var)):
    print("  {0:<22}".format(var) + " = " + tmp[var])    
print(f'')
print(f'Dates:')    
print("  {0:<22}".format('proj_preg_start_date') + " = " + f'{proj_preg_start_date}') 
print("  {0:<22}".format('proj_preg_end_date') + " = " + f'{proj_preg_end_date}') 
print("  {0:<22}".format('proj_fu_end_date') + " = " + f'{proj_fu_end_date}') 
