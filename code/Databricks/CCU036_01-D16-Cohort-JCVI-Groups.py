# Databricks notebook source
# MAGIC %md  
# MAGIC **Description**:
# MAGIC
# MAGIC Derrives variables for assigning Joint Comittee on Vaccination and Immunisation (JCVI) groups with their eligibility dates for patients
# MAGIC
# MAGIC The end result is an adapted JCVI implementation that omits the use of carehome/residential status due to the data not being available in the DSA and relies more on age for some JCVI groups.
# MAGIC
# MAGIC **Author**: Arun Adapted from  the work of Zach Welshman for ccu013_03
# MAGIC
# MAGIC **Reviewed**:
# MAGIC Unreviewed
# MAGIC
# MAGIC <!-- **Relevent Literature/Code**:
# MAGIC https://github.com/opensafely/post-covid-vaccinated/blob/main/analysis/grouping_variables.py  -->

# COMMAND ----------

# MAGIC %run "/Shared/SHDS/common/zw_funcs"

# COMMAND ----------

# MAGIC %run "/Repos/arun.karthikeyansuseeladevi@bristol.ac.uk/ccu036_01/CCU036_01-D01-parameters"  

# COMMAND ----------

# cohort = path_cohort
# vacc = path_vacc
gdppr = path_gdppr

# COMMAND ----------

cohort_to_process = 'cohort'
cohort_to_process 

# COMMAND ----------

# MAGIC %md ## Data

# COMMAND ----------

spark.sql(f"""REFRESH TABLE {dbc_new}.{proj}_cur_vacc""")
vacc   = spark.table(f'{dbc_new}.{proj}_cur_vacc')

# COMMAND ----------

cohort=spark.table(path_cohort)
gdppr=spark.table(path_gdppr)

# COMMAND ----------

# MAGIC %md ##Prepare

# COMMAND ----------

print(proj_preg_start_date,proj_fu_end_date)


# COMMAND ----------

# MAGIC %md ###Vaccination

# COMMAND ----------

vacc_products_of_interest = ['AstraZeneca', 'Moderna','Pfizer']
list_bc = sc.broadcast(vacc_products_of_interest)

vacc = (vacc
        .filter(F.col('PRODUCT').isin(list_bc.value))
        .withColumn('_pivot', F.concat_ws('_', "PRODUCT", "dose_sequence")) #concatenting prod and dose to pivot
        .groupBy('PERSON_ID')
        .pivot('_pivot')
        .agg(F.first('date').alias('date'))
       )

vacc_prods_three_doses = [col for col in vacc.columns 
                          if col.endswith(('_1','_2','_3')) #Doses
                          and col.startswith(('AstraZeneca', 'Moderna','Pfizer'))#products
                         ]

vacc_cols = vacc_prods_three_doses

vacc_cols.append('PERSON_ID')

vacc = vacc.select(vacc_cols)

vacc = (vacc
          .withColumn('first_vaccination_date', 
                      F.array_min(
                        F.array_sort(
                          F.array([vacc[col_name] 
                                   for col_name in vacc.columns 
                                   if 'PERSON_ID' not in col_name]) 
                                 
                                  )
                                )
                     )
         ) #minimum value of a sorted array of product and dose columns
        
display(vacc)


# COMMAND ----------


# create dates
cohort_with_vacc = (cohort.join(vacc.select('PERSON_ID', 'first_vaccination_date'), on='PERSON_ID', how='left'))

display(cohort_with_vacc )


# COMMAND ----------

# MAGIC  %md ## JCVI Group Definition
# MAGIC  - Longres status not avaiable in current cut of data, subsequently JCVI group 1 removed. 
# MAGIC  - Cev, preg and atrisk patients to be derived.
# MAGIC  - Eligilbity dates assigned based on JCVI group and age

# COMMAND ----------

# vax_jcvi_groups
vax_jcvi_groups_data = [
  ("01", "longres_group AND vax_jcvi_age_1 > 65"),
  ("02", "vax_jcvi_age_1 >=80"),
  ("03", "vax_jcvi_age_1 >=75"),
  ("04", "vax_jcvi_age_1 >=70 OR (cev_group AND vax_jcvi_age_1 >=16 AND NOT preg_group)"),
  ("05", "vax_jcvi_age_1 >=65"),
  ("06", "atrisk_group AND vax_jcvi_age_1 >=16"),
  ("07", "vax_jcvi_age_1 >=60"),
  ("08", "vax_jcvi_age_1 >=55"),
  ("09", "vax_jcvi_age_1 >=50"),
  ("10", "vax_jcvi_age_2 >=40"),
  ("11", "vax_jcvi_age_2 >=30"),
  ("12", "vax_jcvi_age_2 >=18"),
  ("99", "DEFAULT")]

# Create a DataFrame
vax_jcvi_groups_ref = spark.createDataFrame(vax_jcvi_groups_data, ["group", "definition"])
vax_jcvi_groups_ref.show(15, truncate = False)

# COMMAND ----------

# MAGIC %md ## JCVI Group Eligible Dates

# COMMAND ----------


# vax_eligbile_dates
vax_eligbile_dates_data = [
  ("2020-12-08", "vax_cat_jcvi_group='01' OR vax_cat_jcvi_group='02'", "01, 02"),
  ("2021-01-18", "vax_cat_jcvi_group='03' OR vax_cat_jcvi_group='04'", "03, 04"),
  ("2021-02-15", "vax_cat_jcvi_group='05' OR vax_cat_jcvi_group='06'", "05, 06"),
  ("2021-02-22", "vax_jcvi_age_1 >= 64 AND vax_jcvi_age_1 < 65", "07"),
  ("2021-03-01", "vax_jcvi_age_1 >= 60 AND vax_jcvi_age_1 < 64", "07"),
  ("2021-03-08", "vax_jcvi_age_1 >= 56 AND vax_jcvi_age_1 < 60", "08"),
  ("2021-03-09", "vax_jcvi_age_1 >= 55 AND vax_jcvi_age_1 < 56", "08"),
  ("2021-03-19", "vax_jcvi_age_1 >= 50 AND vax_jcvi_age_1 < 55", "09"),
  ("2021-04-13", "vax_jcvi_age_2 >= 45 AND vax_jcvi_age_1 < 50", "10"),
  ("2021-04-26", "vax_jcvi_age_2 >= 44 AND vax_jcvi_age_1 < 45", "10"),
  ("2021-04-27", "vax_jcvi_age_2 >= 42 AND vax_jcvi_age_1 < 44", "10"),
  ("2021-04-30", "vax_jcvi_age_2 >= 40 AND vax_jcvi_age_1 < 42", "10"),
  ("2021-05-13", "vax_jcvi_age_2 >= 38 AND vax_jcvi_age_2 < 40", "11"),
  ("2021-05-19", "vax_jcvi_age_2 >= 36 AND vax_jcvi_age_2 < 38", "11"),
  ("2021-05-21", "vax_jcvi_age_2 >= 34 AND vax_jcvi_age_2 < 36", "11"),
  ("2021-05-25", "vax_jcvi_age_2 >= 32 AND vax_jcvi_age_2 < 34", "11"),
  ("2021-05-26", "vax_jcvi_age_2 >= 30 AND vax_jcvi_age_2 < 32", "11"),
  ("2021-06-08", "vax_jcvi_age_2 >= 25 AND vax_jcvi_age_2 < 30", "12"),
  ("2021-06-15", "vax_jcvi_age_2 >= 23 AND vax_jcvi_age_2 < 25", "12"),
  ("2021-06-16", "vax_jcvi_age_2 >= 21 AND vax_jcvi_age_2 < 23", "12"),
  ("2021-06-18", "vax_jcvi_age_2 >= 18 AND vax_jcvi_age_2 < 21", "12"),
  ("2100-12-31", "DEFAULT", "NA")]

vax_eligbile_dates_ref = spark.createDataFrame(vax_eligbile_dates_data, ["eligibility_date", "description", "jcvi_groups"])
vax_eligbile_dates_ref.show(50, truncate = False)


# COMMAND ----------

# MAGIC  %md ## JCVI Reference Dates

# COMMAND ----------

# vax_jcvi_age_1 is the age as of phase 1
ref_age_1 = "2021-03-31"
# vax_jcvi_age_2 is the age as of  phase 2
ref_age_2 = "2021-07-01"
# Reference CEV date
ref_cev = "2021-01-18"
# Reference at risk date
ref_ar = "2021-02-15"

# COMMAND ----------

# MAGIC  %md ## CEV Flag
# MAGIC  Most recent date of a patient with high risk cev status before the ref_cev date

# COMMAND ----------

ref_cev

# COMMAND ----------

#unique patient, date, and cev high risk code.
cev_flag =  (gdppr.select(F.col('NHS_NUMBER_DEID').alias('PERSON_ID'),  
                          F.col('CODE').alias('CODE_CEV'), 
                          F.col('DATE').alias('DATE_CEV'))
             .filter(F.col('CODE') == '1300561000000107')
             .withColumn('TERM_CEV', 
                         F.lit("""High risk category for developing complication
                         from coronavirus disease 19 caused by severe acute respiratory 
                         syndrome coronavirus 2 infection"""))
             .withColumn("CEV_FLAG", F.lit(1))
            )

  
#latest_cev_event
latest_cev_event = get_hx_and_latest_event_by_cols(cev_flag , 
                                                 'date_cev', 
                                                 ref_cev, 
                                                 'PERSON_ID')


# COMMAND ----------

# MAGIC  %md ### Cohort with CEV Flag

# COMMAND ----------


cohort_with_cev = cohort_with_vacc.join(latest_cev_event.select('person_id',
                                                      'code_cev',
                                                      'date_cev',
                                                      'term_cev',
                                                      'cev_flag'), 
                              on='PERSON_ID', how='left')


# COMMAND ----------

cohort_with_cev.select(F.count('person_id'), F.countDistinct('person_id')).show()

# COMMAND ----------

# saving cohort with cev checkpoint
outName = f'{proj}_{cohort_to_process}_with_cev'.lower()
cohort_with_cev = cohort_with_cev.repartition(50)
cohort_with_cev.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f'{dbc_new}.{outName}')


# COMMAND ----------

# loading cohort with cev checkpoint
cohort_with_cev = spark.table(f'{dbc_new}.{proj}_{cohort_to_process}_with_cev')
cohort_with_cev.cache().count()

# COMMAND ----------

# MAGIC  %md 
# MAGIC  ## Pregnancy Flag
# MAGIC  (preg_36wks_date AND cov_cat_sex = 'F' AND vax_jcvi_age_1 < 50) AND
# MAGIC  (pregdel_pre_date <= preg_36wks_date OR NOT pregdel_pre_date)

# COMMAND ----------

# MAGIC %run "/Repos/arun.karthikeyansuseeladevi@bristol.ac.uk/ccu036_01/CCU036_01-D02f-codelist_preg_group"

# COMMAND ----------

# 36 week pregnancy codes, gdppr joined to preg_sdf
preg_36wks_date_flag = (gdppr.select(F.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 
                              F.col('code').alias('CODE_PREG'), 
                              F.col('date').alias('DATE_PREG'), 
                              'VALUE1_CONDITION')
                        
                         .withColumn('REF_CEV_DATE', F.lit(ref_cev))

                         .filter(F.col('DATE') >= F.date_sub(F.col('REF_CEV_DATE'), 252))

                         .join(preg_sdf.select(F.col('code').alias('CODE_PREG'),
                                               F.col('term').alias('TERM_PREG')), 
                               on='CODE_PREG', 
                               how='inner')

                         .withColumn('PREG_FLAG', 
                                     F.lit(1)
                                    )
                    )

latest_preg_36wks_date_event = get_hx_and_latest_event_by_cols(preg_36wks_date_flag , 
                                                              'DATE_PREG', 
                                                              ref_cev, 
                                                              'PERSON_ID')


# COMMAND ----------


pregdel_pre_date_flag = (gdppr.select(F.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 
                              F.col('code').alias('CODE_PREG_DEL'), 
                              F.col('date').alias('DATE_PREG_DEL'), 
                                  'VALUE1_CONDITION')

                         .withColumn('REF_CEV_DATE', F.lit(ref_cev))

                         .filter(gdppr['DATE'] >= F.date_sub(F.col('REF_CEV_DATE'), 252))

                         .join(preg_del_sdf.select(F.col('code').alias('CODE_PREG_DEL'),
                                               F.col('term').alias('TERM_PREG_DEL')), 
                               on='CODE_PREG_DEL', 
                               how='inner')

                         .withColumn('PREG_DEL_FLAG', 
                                     F.lit(1)
                                    )
                    )

latest_pregdel_pre_date_event  = get_hx_and_latest_event_by_cols(pregdel_pre_date_flag , 
                                    'DATE_PREG_DEL', 
                                    ref_cev, 
                                    'PERSON_ID')
                                 


# COMMAND ----------


preg_del_date_less_than_or_not_equal_to_preg_date = (
  F.when(
    (F.col('DATE_PREG_DEL') <= F.col('DATE_PREG'))
    | (F.col('DATE_PREG_DEL').isNull() & F.col('DATE_PREG').isNotNull())
    | (F.col('DATE_PREG_DEL') != F.col('DATE_PREG')), 1).otherwise(99))

preg_flag_derrived = (latest_preg_36wks_date_event
                      
                     .join(latest_pregdel_pre_date_event, 
                           on='PERSON_ID',
                           how='left')
                      
                     .withColumn('PREG_AND_PREG_DEL_FLAG', 
                                 preg_del_date_less_than_or_not_equal_to_preg_date)
                     )


# COMMAND ----------


preg_flag_derrived = preg_flag_derrived.select('PERSON_ID', 
                                               'CODE_PREG',
                                               'DATE_PREG',
                                               'TERM_PREG',
                                               'PREG_FLAG',
                                               'CODE_PREG_DEL',
                                                'DATE_PREG_DEL',
                                                'TERM_PREG_DEL',
                                               'PREG_DEL_FLAG',
                                              'PREG_AND_PREG_DEL_FLAG')


cohort_with_cev_preg = cohort_with_cev.join(preg_flag_derrived, 
                                            on='PERSON_ID' ,
                                            how='left')


# COMMAND ----------

# MAGIC  %md ## At Risk Flag
# MAGIC
# MAGIC 

# COMMAND ----------

ref_ar

# COMMAND ----------

# MAGIC %md ### Import at risk codes

# COMMAND ----------

# MAGIC  %run ./CCU036_01-D02e-codelist_atrisk_group

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timedelta, date

def flag_within_days_from_date(sdf, 
                               date_col,
                              start_date, 
                              days_from_start_date, 
                              end_date,
                               flag_column_name,
                              df_with_codes,
                              ):
    """
    Returns: 
      A SparkDataFrame with patients and  flags that 
      have a codematch within days from a startdate.


     sparkDataFrame with columns:
      NHS_NUMBER_DEID
      DATE
      CODE_DATASET: code match in the dataset
      NAME_FLAG: name of the flag from flag_column_name,
      NAME_FLAG_WITHIN_DATE_RANGE: name of the flag from 
                    flag_column_name and within dateflag
    Args:
     sdf: spark dataframe to find code matches
     date_col: column to use as date values
     start_date: start date for looking back
     days_from_start_date: number of days to look back 
     end_date: the furthest possible time to look back
     flag_column_name: name to assign to the flag
     df_with_codes: the spark dataframe with codes to 
     match on sdf

    """


    df_filt = (sdf
              
               
            .withColumn(f'{flag_column_name}_FLAG_START_DATE', F.lit(start_date))
            .withColumn(f'{flag_column_name}_FLAG_WITHIN_DATE_RANGE',
                        F.when( F.col(date_col).between(
                        (F.date_sub(F.col(f'{flag_column_name}_FLAG_START_DATE'), days_from_start_date)) , 
                        end_date), 1).otherwise(None))
            .filter(F.col(f'{flag_column_name}_FLAG_WITHIN_DATE_RANGE') == 1)

            .join(df_with_codes, 
                  sdf[f'{flag_column_name}_CODE_DATASET'] == df_with_codes['CODE_CODELIST'],
                   how='inner')

            

            
            
            .withColumn(f'{flag_column_name}_FLAG', F.lit(1))
           
            )
            
    #   df_ret = df_filt.
    return df_filt.select('NHS_NUMBER_DEID',
                           f'{flag_column_name}_DATE',
                           f'{flag_column_name}_CODE_DATASET',
                           'VALUE1_CONDITION')


# COMMAND ----------


## at_risk_gdppr for matching
at_risk_gdppr = (
    gdppr.select('NHS_NUMBER_DEID', 'CODE', 'DATE','VALUE1_CONDITION')
)


# COMMAND ----------



at_risk_codelist_attributes = {
    'astadm': {
        'csv_name' : 'astadm.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 730,
        'end_date': '2021-02-14',
        
    },
    'ast': {
        'csv_name' : 'ast.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date': '2021-02-14',
    },

    'astrxm1': {
        'csv_name' : 'astrx.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 31,
        'end_date': '2021-02-14',
        
    },
    'astrxm2': {
        'csv_name' : 'astrx.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 61,
        'end_date': str(date(2021, 2, 15) -  timedelta(days = 32)),# 2021-02-15 minus 32 days
        
    },
    'astrxm3': {
        'csv_name' : 'astrx.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 91,
        'end_date':  str(date(2021, 2, 15) -  timedelta(days = 62)),# 2021-02-15 minus 62 days
        
    },
    'resp': {
        'csv_name' : 'resp.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },
    'cns': {
        'csv_name' : 'cns.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },
    'diab': {
        'csv_name' : 'diab.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },

    'dmres': {
        'csv_name' : 'dmres.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 9999,
        'end_date':  '2021-02-14',
    },

    'sev_mental': {
        'csv_name' : 'sev_mental.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },
    'smhres': {
        'csv_name' : 'smhres.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },
    'chd': {
        'csv_name' : 'chd.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },
    'ckd_15': {
        'csv_name' : 'ckd_15.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },
    'ckd_35': {
        'csv_name' : 'ckd_35.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },
    'ckd': {
        'csv_name' : 'ckd.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },
    'cld': {
        'csv_name' : 'cld.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },
    'immrx': {
        'csv_name' : 'immrx.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },
    'immdx': {
        'csv_name' : 'immdx.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },
    'spln': {
        'csv_name' : 'spln.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },
    'learndis': {
        'csv_name' : 'learndis.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },
    'bmi_stage': {
        'csv_name' : 'bmi_stage.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },
    'sev_obesity': {
        'csv_name' : 'sev_obesity.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },
    'bmi': {
        'csv_name' : 'bmi.csv',
        'start_date': '2021-02-15',
        'days_from_start_date': 99999,
        'end_date':  '2021-02-14',
    },
}


# COMMAND ----------

at_risk_dfs = []

for codelist_name in list(at_risk_codelist_attributes.keys()):
    
    codelist =  at_risk_codelist_attributes.get(codelist_name)
    print(codelist)
    
    flags = flag_within_days_from_date(
        (at_risk_gdppr
               .withColumnRenamed('DATE', f'{codelist_name}_DATE')
               .withColumnRenamed('CODE',  f'{codelist_name}_CODE_DATASET')),
        date_col=f'{codelist_name}_date', 
        start_date = codelist.get('start_date'), #starting date for ever had a code
        days_from_start_date=codelist.get('days_from_start_date'),
        end_date = codelist.get('end_date'), # in some cases it is end_date = ref_ar - 62 days etc see at_risk_codelist_attributes
        flag_column_name = codelist_name,

        df_with_codes = (at_risk_sdf
                        .withColumnRenamed('code', 'CODE_CODELIST')
                        .where(F.col('SOURCE')
                                .contains(codelist.get('csv_name'))
                                )
                        )
        )

    latest_flags = get_hx_and_latest_event_by_cols(
        flags,
        f'{codelist_name}_date',
        'NHS_NUMBER_DEID')
    
    if 'bmi_DATE' in latest_flags.columns:
        latest_flags = (
            latest_flags
            .withColumn('BMI_RISK_FLAG',
                        F.when((F.col('VALUE1_CONDITION') > 40) & (F.col('VALUE1_CONDITION') < 100), 1)
                    .otherwise(None))
                    .where(F.col('BMI_RISK_FLAG') == 1) #Keeping values between 40 and 100
                    .select('NHS_NUMBER_DEID', 
                                f'{codelist_name}_DATE',
                                f'{codelist_name}_CODE_DATASET',
                                'VALUE1_CONDITION')
                        )
        at_risk_dfs.append(latest_flags)
    
    else:
       (at_risk_dfs
        .append(latest_flags
                .select('NHS_NUMBER_DEID', 
                        f'{codelist_name}_DATE',
                         f'{codelist_name}_CODE_DATASET')))
        
            
        



# COMMAND ----------

at_risk_dfs[-1]

# COMMAND ----------

# display(at_risk_dfs[-1])

# COMMAND ----------

init_ids = cohort.select('PERSON_ID')

for df in at_risk_dfs:
    init_ids = (init_ids
                .join(df, 
                      init_ids['PERSON_ID']==df['NHS_NUMBER_DEID'],
                       how='left')
                .drop('NHS_NUMBER_DEID')
    )

# COMMAND ----------

init_ids

# COMMAND ----------

# display(init_ids)

# COMMAND ----------

# at_risk

# COMMAND ----------


#gathering list of date columns where they are not none then 
whens = [
    F.when(F.col(col).isNotNull(),1).otherwise(None) 
    for col in init_ids.columns 
    if 'PERSON_ID' not in col 
    and col.endswith('DATE') 
    or 'CODE' in col
    or 'VALUE1_CONDITION' in col]
whens

# COMMAND ----------

# at_risk

# COMMAND ----------


all_at_risk_flags = (
    init_ids
    .withColumn('RISK_FLAG', 
                F.array_max(
                    F.array(whens))
                )
    )


# COMMAND ----------

# display(all_at_risk_flags)

# COMMAND ----------

# saving all_at_risk_flags checkpoint
outName = f'{proj}_{cohort_to_process}_all_at_risk_flags'.lower()

(all_at_risk_flags
 .write
 .mode('overwrite')
 .option("overwriteSchema", "true")
 .saveAsTable(f'{dbc_new}.{outName}')
)


# COMMAND ----------

# MAGIC  %md
# MAGIC  AT Risk Flag Partitions

# COMMAND ----------


sc.setJobGroup('JCVI', 'JCVI_to_cev_preg_join')

cohort_with_cev_preg_atrisk =  (cohort_with_cev_preg
                                
                               .join(all_at_risk_flags
                                     .select('PERSON_ID','RISK_FLAG'), 
                                     on='PERSON_ID', 
                                     how='left'))
                                
# validation_groupby_percentage(cohort_with_cev_preg_atrisk, 
#                               'PERSON_ID', #value_col
#                               'AT_RISK_FLAG'
#                              ).show()                              



# COMMAND ----------

# MAGIC  %md ## Check

# COMMAND ----------

cohort_with_cev_preg_atrisk.printSchema()

# COMMAND ----------

# saving cohort with cev risk checkpoint
outName = f'{proj}_{cohort_to_process}_with_cev_preg_atrisk'.lower()
cohort_with_cev_preg_atrisk = cohort_with_cev_preg_atrisk.repartition(50)
cohort_with_cev_preg_atrisk.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f'{dbc_new}.{outName}')

# COMMAND ----------

# loading cohort with cev at risk checkpoint
cohort_with_cev_preg_atrisk = spark.table(f'{dbc_new}.{proj}_{cohort_to_process}_with_cev_preg_atrisk')

# COMMAND ----------



# cohort_with_cev_preg_atrisk.select(F.count('person_id'), F.countDistinct('person_id')).show()
print(f"Total in cohort with 'RISK_FLAG' {cohort_with_cev_preg_atrisk.select('person_id', 'RISK_FLAG').filter(f.col('RISK_FLAG')==1).count()}")

# COMMAND ----------

# MAGIC  %md ## Distributions of Partitions of CEV and at risk 

# COMMAND ----------


# sc.setJobGroup('JCVI', 'JCVI_to_cev_join repartition')
                                          
# cohort_with_cev_preg_atrisk_partitons = (cohort_with_cev_preg_atrisk.groupBy(F.spark_partition_id())
#                                                                     .count()
#                                                                     .orderBy(F.desc("count"))) 
# display(cohort_with_cev_preg_atrisk_partitons)

# COMMAND ----------

# MAGIC %md ## Assigning JCVI groups to patients

# COMMAND ----------

# cohort_new_samp = cohort_with_cev_preg_atrisk
skinny=spark.table(path_skinny)


cohort_with_cev_preg_atrisk = cohort_with_cev_preg_atrisk.join(skinny.select('PERSON_ID',
                                                      'DOB'), 
                              on='PERSON_ID', how='left')
display(cohort_with_cev_preg_atrisk)

# COMMAND ----------

sc.setJobGroup('JCVI', 'ASSIGNING JCVI flags')
# RD and MA
# Group 1 simplified to age this was due to lack of longres
# Removed Group 5 and it was similar to Group 1
# Removed Group 1 and kept Group 5

vax_jcvi_ref_ages_1_2 = (cohort_with_cev_preg_atrisk
                  .withColumn('REF_AGE_1_DATE', f.lit(ref_age_1)) #creates literal in the column
                  .withColumn('VAX_JCVI_AGE_1',  f.datediff(f.col('REF_AGE_1_DATE'), f.col('DOB')) / 365.25) # difference between date in days and divides by 365.25 for years
                  .withColumn('VAX_JCVI_AGE_1', f.round(f.col('VAX_JCVI_AGE_1'), 2)) # Rounds the age in years
                  
                  .withColumn('REF_AGE_2_DATE', f.lit(ref_age_2)) #creates literal in the column
                  .withColumn('VAX_JCVI_AGE_2',  f.datediff(f.col('REF_AGE_2_DATE'), f.col('DOB')) / 365.25) # difference between date in days and divides by 365.25 for years
                  .withColumn('VAX_JCVI_AGE_2', f.round(f.col('VAX_JCVI_AGE_2'), 2)) # Rounds the age in years
                 ) 


# JCVI_GROUP_1 = (F.when(vax_jcvi_ref_ages_1_2['VAX_JCVI_AGE_1'] > 65, 1).otherwise(None))# longres_group AND vax_jcvi_age_1 > 65 cant get longres
JCVI_GROUP_2 = (F.when(vax_jcvi_ref_ages_1_2['VAX_JCVI_AGE_1'] >= 80, 2).otherwise(None)) # vax_jcvi_age_1 >=80
JCVI_GROUP_3 = (F.when(vax_jcvi_ref_ages_1_2['VAX_JCVI_AGE_1'] >= 75, 3).otherwise(None)) # vax_jcvi_age_1 >=75

JCVI_GROUP_4 = (
  F.when(
  (vax_jcvi_ref_ages_1_2['VAX_JCVI_AGE_1'] >= 70) | ((vax_jcvi_ref_ages_1_2['CEV_FLAG'] == 1)
       & (vax_jcvi_ref_ages_1_2['VAX_JCVI_AGE_1'] >= 16)
       & (vax_jcvi_ref_ages_1_2['PREG_AND_PREG_DEL_FLAG'].isNull())), 4).otherwise(None))# vax_jcvi_age_1 >=70 OR (cev_group AND vax_jcvi_age_1 >=16 AND NOT preg_group)

JCVI_GROUP_5 = (F.when(vax_jcvi_ref_ages_1_2['VAX_JCVI_AGE_1'] >= 65, 5).otherwise(None)) # vax_jcvi_age_1 >=80

JCVI_GROUP_6 = (
  F.when((vax_jcvi_ref_ages_1_2['RISK_FLAG'] >= 1)
         & (vax_jcvi_ref_ages_1_2['VAX_JCVI_AGE_1'] >= 16), 6).otherwise(None)
               )
JCVI_GROUP_7 = (F.when(vax_jcvi_ref_ages_1_2['VAX_JCVI_AGE_1'] >= 60, 7).otherwise(None))
JCVI_GROUP_8 = (F.when(vax_jcvi_ref_ages_1_2['VAX_JCVI_AGE_1'] >= 55, 8).otherwise(None))
JCVI_GROUP_9 = (F.when(vax_jcvi_ref_ages_1_2['VAX_JCVI_AGE_1'] >= 50, 9).otherwise(None))
JCVI_GROUP_10 = (F.when(vax_jcvi_ref_ages_1_2['VAX_JCVI_AGE_2'] >= 40, 10).otherwise(None))
JCVI_GROUP_11 = (F.when(vax_jcvi_ref_ages_1_2['VAX_JCVI_AGE_2'] >= 30, 11).otherwise(None))                     
JCVI_GROUP_12 = (F.when(vax_jcvi_ref_ages_1_2['VAX_JCVI_AGE_1'] >= 16, 12).otherwise(None)) 
                          

cohort_vax_jcvi_groups_omitted_1 = (vax_jcvi_ref_ages_1_2
#                           .withColumn('JCVI_GROUP_1', JCVI_GROUP_1)
                          .withColumn('JCVI_GROUP_2', JCVI_GROUP_2) 
                          .withColumn('JCVI_GROUP_3', JCVI_GROUP_3) 
                          .withColumn('JCVI_GROUP_4', JCVI_GROUP_4)
                          .withColumn('JCVI_GROUP_5', JCVI_GROUP_5)
                          .withColumn('JCVI_GROUP_6', JCVI_GROUP_6)
                          .withColumn('JCVI_GROUP_7', JCVI_GROUP_7)
                          .withColumn('JCVI_GROUP_8', JCVI_GROUP_8)
                          .withColumn('JCVI_GROUP_9', JCVI_GROUP_9)
                          .withColumn('JCVI_GROUP_10',JCVI_GROUP_10)
                          .withColumn('JCVI_GROUP_11', JCVI_GROUP_11)       
                          .withColumn('JCVI_GROUP_12', JCVI_GROUP_12)         
                         )

display(cohort_vax_jcvi_groups_omitted_1)


# COMMAND ----------

jcvi_col_names = [col for col in cohort_vax_jcvi_groups_omitted_1.columns if col.startswith('JCVI_GROUP')]
jcvi_col_names

# COMMAND ----------


#refactor list comprehension
arr_cohort_vax_jcvi_groups_omitted_1 = F.array(
#   cohort_vax_jcvi_groups['JCVI_GROUP_1'].cast('integer'),
                   cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_2'].cast('integer'),
                   cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_3'].cast('integer'),
                   cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_4'].cast('integer'),
                   cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_5'].cast('integer'),
                   cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_6'].cast('integer'),
                   cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_7'].cast('integer'),
                   cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_8'].cast('integer'),
                   cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_9'].cast('integer'),
                   cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_10'].cast('integer'),
                   cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_11'].cast('integer'),
                   cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_12'].cast('integer')
                   )

cohort_vax_jcvi_groups_omitted_1 = cohort_vax_jcvi_groups_omitted_1.withColumn("JCVI_GROUP_MIN", F.array_min(arr_cohort_vax_jcvi_groups_omitted_1))

display(cohort_vax_jcvi_groups_omitted_1 )          

# COMMAND ----------


(cohort_vax_jcvi_groups_omitted_1
 .select('PERSON_ID','JCVI_GROUP_MIN')
 .groupby(f.col('JCVI_GROUP_MIN'))
 .agg(F.count('PERSON_ID'))
 .orderBy(F.asc('JCVI_GROUP_MIN'))).show()


# COMMAND ----------

# MAGIC  %md ## Assigning eligible dates to JCVI Groups

# COMMAND ----------

# display(vax_eligbile_dates_ref)

# COMMAND ----------


display(cohort_vax_jcvi_groups_omitted_1)

# COMMAND ----------

sc.setJobGroup('JCVI', 'ASSIGNING JCVI eligibilty dates')
# TODO Refactor from line 43

JCVI_GROUP_01_02_ELIGIBILITY_DATE = (
  F.when(
  (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN'] == 1 )
   | (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN'] == 2), "2020-12-08").otherwise(None))

JCVI_GROUP_03_04_ELIGIBILITY_DATE = (
  F.when(
    (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']== 3 )
    | (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']== 4 ), "2021-01-18").otherwise(None))

JCVI_GROUP_05_06_ELIGIBILITY_DATE = (
  F.when(
      (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']== 5 )
     | (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']== 6 ), "2021-02-15").otherwise(None))

jcvi_group_7_vax_jcvi_age_1_greater_equal_64_less_65 = ( 
  F .when(
    (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']== 7)
    & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_1'] >=64)
    & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_1'] < 65), "2021-02-22").otherwise(None))

jcvi_group_7_vax_jcvi_age_1_greater_equal_60_less_64 = ( 
  F .when(
         (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']==7)
          & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_1'] >=60)
          & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_1'] < 64), "2021-03-01").otherwise(None))

eligible_dates_vax_jcvi_groups_omitted_1 = (cohort_vax_jcvi_groups_omitted_1
                                    
                                     .withColumn('JCVI_GROUP_01_02_ELIGIBILITY_DATE', JCVI_GROUP_01_02_ELIGIBILITY_DATE)
                                     .withColumn('JCVI_GROUP_03_04_ELIGIBILITY_DATE', JCVI_GROUP_03_04_ELIGIBILITY_DATE)                                        
                                     .withColumn('JCVI_GROUP_05_06_ELIGIBILITY_DATE',JCVI_GROUP_05_06_ELIGIBILITY_DATE) 
                                     .withColumn('jcvi_group_7_vax_jcvi_age_1_greater_equal_64_less_65', 
                                                 jcvi_group_7_vax_jcvi_age_1_greater_equal_64_less_65) 
                                  
                                    .withColumn('jcvi_group_7_vax_jcvi_age_1_greater_equal_60_less_64', 
                                                jcvi_group_7_vax_jcvi_age_1_greater_equal_60_less_64)
                                                 
                                  
                                  .withColumn('jcvi_group_8_vax_jcvi_age_1_greater_equal_56_less_60',
                                                  F .when(
                                                       (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']==8)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_1'] >=56)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_1'] < 60), "2021-03-08").otherwise(None))
                                  
                                  .withColumn('jcvi_group_8_vax_jcvi_age_1_greater_equal_55_less_56',
                                                  F .when(
                                                       (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']==8)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_1'] >=55)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_1'] < 56), "2021-03-09").otherwise(None))
                                  
                                  .withColumn('jcvi_group_9_vax_jcvi_age_1_greater_equal_50_less_55',
                                                  F .when(
                                                       (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN'] == 9)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_1'] >=50)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_1'] < 55), "2021-03-19").otherwise(None))
                                  
                                  .withColumn('jcvi_group_10_vax_jcvi_age_2_greater_equal_45_vax_jcvi_age_1_less_50',
                                                  F .when(
                                                       (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']==10)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] >=45)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_1'] < 50), "2021-04-13").otherwise(None))
                                  
                                  .withColumn('jcvi_group_10_vax_jcvi_age_2_greater_equal_44_vax_jcvi_age_1_less_45',
                                                  F .when(
                                                       (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']==10)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] >=44)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_1'] < 45), "2021-04-26").otherwise(None))
                                  
                                  .withColumn('jcvi_group_10_vax_jcvi_age_2_greater_equal_42_vax_jcvi_age_1_less_44',
                                                  F .when(
                                                       (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']==10)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] >=42)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_1'] < 44), "2021-04-27").otherwise(None))
                                  
                                  .withColumn('jcvi_group_10_vax_jcvi_age_2_greater_equal_40_vax_jcvi_age_1_less_42',
                                                  F .when(
                                                       (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']==10)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] >=40)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_1'] < 42), "2021-04-30").otherwise(None))
                                  
                                  .withColumn('jcvi_group_11_vax_jcvi_age_2_greater_equal_38_vax_less_40',
                                                  F .when(
                                                       (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN'] ==11)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] >=38)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] < 40), "2021-05-13").otherwise(None))
                                   
                                  .withColumn('jcvi_group_11_vax_jcvi_age_2_greater_equal_36_vax_less_38',
                                                  F .when(
                                                       (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']==11)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] >=36)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] < 38), "2021-05-19").otherwise(None)) 
                                  
                                  .withColumn('jcvi_group_11_vax_jcvi_age_2_greater_equal_34_vax_less_36',
                                                  F .when(
                                                       (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']==11)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] >=34)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] < 36), "2021-05-21").otherwise(None)) 
                                  
                                  .withColumn('jcvi_group_11_vax_jcvi_age_2_greater_equal_32_vax_less_34',
                                                  F .when(
                                                       (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']==11)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] >=32)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] < 34), "2021-05-25").otherwise(None)) 
                                   
                                  .withColumn('jcvi_group_11_vax_jcvi_age_2_greater_equal_30_vax_less_32',
                                                  F .when(
                                                       (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']==11)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] >=30)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] < 32), "2021-05-26").otherwise(None)) 
                                  
                                  .withColumn('jcvi_group_12_vax_jcvi_age_2_greater_equal_25_vax_less_30',
                                                  F .when(
                                                       (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN'] == 12)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] >=25)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] < 30), "2021-06-08").otherwise(None)) 
                                  
                                   
                                  .withColumn('jcvi_group_12_vax_jcvi_age_2_greater_equal_23_vax_less_25',
                                                  F .when(
                                                       (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN'] == 12)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] >=23)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] < 25), "2021-06-15").otherwise(None))
                                  
                                  .withColumn('jcvi_group_12_vax_jcvi_age_2_greater_equal_21_vax_less_23',
                                                  F .when(
                                                       (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']==12)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] >=21)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] < 23), "2021-06-16").otherwise(None))
                                  
                                  .withColumn('jcvi_group_12_vax_jcvi_age_2_greater_equal_18_vax_less_21',
                                                  F .when(
                                                       (cohort_vax_jcvi_groups_omitted_1['JCVI_GROUP_MIN']==12)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] >=18)
                                                        & (cohort_vax_jcvi_groups_omitted_1['vax_jcvi_age_2'] < 21), "2021-06-18").otherwise(None))
                                  
                                  
                                 )
                                                                              
#refactor to list comp                                                                                 
arr_eligible_dates_vax_jcvi_groups_omitted_1 = F.array(eligible_dates_vax_jcvi_groups_omitted_1['JCVI_GROUP_01_02_ELIGIBILITY_DATE'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['JCVI_GROUP_03_04_ELIGIBILITY_DATE'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['JCVI_GROUP_05_06_ELIGIBILITY_DATE'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_7_vax_jcvi_age_1_greater_equal_64_less_65'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_7_vax_jcvi_age_1_greater_equal_60_less_64'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_8_vax_jcvi_age_1_greater_equal_56_less_60'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_8_vax_jcvi_age_1_greater_equal_55_less_56'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_9_vax_jcvi_age_1_greater_equal_50_less_55'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_10_vax_jcvi_age_2_greater_equal_45_vax_jcvi_age_1_less_50'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_10_vax_jcvi_age_2_greater_equal_44_vax_jcvi_age_1_less_45'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_10_vax_jcvi_age_2_greater_equal_42_vax_jcvi_age_1_less_44'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_10_vax_jcvi_age_2_greater_equal_40_vax_jcvi_age_1_less_42'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_11_vax_jcvi_age_2_greater_equal_38_vax_less_40'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_11_vax_jcvi_age_2_greater_equal_36_vax_less_38'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_11_vax_jcvi_age_2_greater_equal_34_vax_less_36'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_11_vax_jcvi_age_2_greater_equal_32_vax_less_34'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_11_vax_jcvi_age_2_greater_equal_30_vax_less_32'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_12_vax_jcvi_age_2_greater_equal_25_vax_less_30'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_12_vax_jcvi_age_2_greater_equal_23_vax_less_25'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_12_vax_jcvi_age_2_greater_equal_21_vax_less_23'].cast('date'),
                                             eligible_dates_vax_jcvi_groups_omitted_1['jcvi_group_12_vax_jcvi_age_2_greater_equal_18_vax_less_21'].cast('date'),
                                             
                                 )
# TODO rename date columns for list comprehension
# arr_eligible_dates_vax_jcvi_groups_omitted_1 = f.array([col_to_cast_date.cast('date') 
#                                                         for col_to_cast_date in eligible_dates_vax_jcvi_groups_omitted_1.columns 
#                                                         if col_to_cast_date.startswith('eligibility_date_jcvi_group')]

eligbile_dates_cohort_vax_jcvi_groups_omitted_1 = (eligible_dates_vax_jcvi_groups_omitted_1
                                                   .withColumn("ELIGIBLE_DATE_JCVI_GROUP_MIN",
                                                               F.array_min(arr_eligible_dates_vax_jcvi_groups_omitted_1 ))
                                                  )

display(eligbile_dates_cohort_vax_jcvi_groups_omitted_1)



# COMMAND ----------

om1 = (eligbile_dates_cohort_vax_jcvi_groups_omitted_1
        .select('PERSON_ID','ELIGIBLE_DATE_JCVI_GROUP_MIN','JCVI_GROUP_MIN')
        .groupby(F.col('JCVI_GROUP_MIN'),f.col('ELIGIBLE_DATE_JCVI_GROUP_MIN'))
        .agg(F.count('PERSON_ID'))
        .orderBy(F.asc('JCVI_GROUP_MIN'))
        .withColumn('src', F.lit('om1'))).cache()

# COMMAND ----------

display(om1)

# COMMAND ----------

display(om1.agg(F.sum('count(PERSON_ID)')))

# COMMAND ----------

display(eligbile_dates_cohort_vax_jcvi_groups_omitted_1
        .select('PERSON_ID','ELIGIBLE_DATE_JCVI_GROUP_MIN','JCVI_GROUP_MIN')
        .groupby(F.col('JCVI_GROUP_MIN'),f.col('ELIGIBLE_DATE_JCVI_GROUP_MIN'))
        .agg(F.count('PERSON_ID'))
        .orderBy(F.asc('JCVI_GROUP_MIN')))

# COMMAND ----------

chunks_eligbile_dates_cohort_vax_jcvi_groups_omitted_1 = eligbile_dates_cohort_vax_jcvi_groups_omitted_1.withColumn('CHUNK', F.spark_partition_id())

# COMMAND ----------

display(chunks_eligbile_dates_cohort_vax_jcvi_groups_omitted_1)

# COMMAND ----------


# saving cohort with jcvi_checkpoint
# assert 1==0
outName = f'{proj}_{cohort_to_process}_jcvi'.lower()
chunks_eligbile_dates_cohort_vax_jcvi_groups_omitted_1.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f'{dbc_new}.{outName}')
