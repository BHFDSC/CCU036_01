#===============================================================================
## EXTRACTING DATA FROM DATABRICKS AND CREATING THE ANALYSIS DATASET FOR DURING 
## PREGNANCY AND AT BIRTH OUTCOMES
#===============================================================================


# input data --------------------------------------------------------------
## dars_nic_391419_j3w9t_collab.ccu036_01_out_cohort
## dars_nic_391419_j3w9t_collab.ccu036_01_out_covariates
## dars_nic_391419_j3w9t_collab.ccu036_01_out_exposure
## dars_nic_391419_j3w9t_collab.ccu036_01_out_outcomes_dur_preg
## dars_nic_391419_j3w9t_collab.ccu036_01_out_outcomes_post_preg
## dars_nic_391419_j3w9t_collab.ccu036_01_out_outcomes_at_birth
## dars_nic_391419_j3w9t_collab.ccu036_01_out_skinny
## dars_nic_391419_j3w9t_collab.ccu036_01_out_covid
## dsa_391419_j3w9t_collab.ccu036_01_cohort_jcvi


# output file -------------------------------------------------------------
## ccu036_01_analysis_data_13112022.csv 
## ccu036_01_analysis_data_04092023.csv 

#===============================================================================



## This script follows the following seqeunce

#1. Load the packages
#2. Connect to databricks 
#3. Extract the tables one by one
#4. Save the raw data table
#5. Creating the required variables
#6. Combining the dataset to form the analysis dataset




rm(list = ls())

# 1. Load the packages ----------------------------------------------------


# required packages
library(DBI)
library(dbplyr)
library(dplyr)
library(lubridate)
library(data.table)



# 2. Connect to databricks  -----------------------------------------------


#aks token
#dapia3bfce81456ccb7e22511f0dda5b8ac5


# connect to databricks
# this code will prompt you to enter your personal access token which you should have generated following the Connecting to Databricks instructions in the Using RStudio in DAE Guide
con <- dbConnect(odbc::odbc(), 
                 "Databricks", 
                 timeout = 60, 
                 PWD = rstudioapi::askForPassword("Please enter your Databricks personal access token"))


# 3. Extract the tables one by one ----------------------------------------


# cohort
ccu036_cohort_tbl <- tbl(con, in_schema("dars_nic_391419_j3w9t_collab", "ccu036_01_out_cohort"))
cohort <- ccu036_cohort_tbl %>%
  collect()

# covariates
ccu036_covariates_tbl <- tbl(con, in_schema("dars_nic_391419_j3w9t_collab", "ccu036_01_out_covariates"))
covariates <- ccu036_covariates_tbl %>%
  collect()

# covid exposure
ccu036_exposure_tbl <- tbl(con, in_schema("dars_nic_391419_j3w9t_collab", "ccu036_01_out_exposure"))
exposure <- ccu036_exposure_tbl %>%
 collect()

# covid vaccination (NOT NEEDED AS WE HAVE EXPOSURE TABLE)
# ccu036_vaccination_tbl <- tbl(con, in_schema("dars_nic_391419_j3w9t_collab", "ccu036_01_vaccination"))
# vaccination <- ccu036_vaccination_tbl %>%
#   collect()

# outcomes during pregnancy
ccu036_outcomes_dur_preg_tbl <- tbl(con, in_schema("dars_nic_391419_j3w9t_collab", "ccu036_01_out_outcomes_dur_preg"))
outcomes_during_preg <- ccu036_outcomes_dur_preg_tbl %>%
  collect()

# outcomes post pregnancy
ccu036_outcomes_post_preg_tbl <- tbl(con, in_schema("dars_nic_391419_j3w9t_collab", "ccu036_01_out_outcomes_post_preg"))
outcomes_post_preg <- ccu036_outcomes_post_preg_tbl %>%
  collect()

# outcomes at birth
ccu036_outcomes_at_birth_tbl <- tbl(con, in_schema("dars_nic_391419_j3w9t_collab", "ccu036_01_out_outcomes_at_birth"))
outcomes_birth <- ccu036_outcomes_at_birth_tbl %>%
  collect()

# skinny table
ccu036_skinny_tbl <- tbl(con, in_schema("dars_nic_391419_j3w9t_collab", "ccu036_01_out_skinny"))
skinny <- ccu036_skinny_tbl %>%
  collect()

#during pregnancy covid infection
ccu036_covid<- tbl(con, in_schema("dars_nic_391419_j3w9t_collab", "ccu036_01_out_covid"))
skinny <- ccu036_covid %>%
  collect()
covid <- skinny



# 4. Save the raw data table ----------------------------------------------


#setwd("~/CCU036_01/data")
setwd("~/dars_nic_391419_j3w9t_collab/CCU036_01/data")
proj <- 'ccu036_01_'
date <- '13112022'

write.csv(cohort, file = paste0(proj, "cohort",date,".csv"), row.names = TRUE)
write.csv(exposure, file = paste0(proj, "exposure",date,".csv"), row.names = TRUE)
write.csv(covariates, file = paste0(proj, "covariates",date,".csv"), row.names = TRUE)
write.csv(outcomes_birth, file = paste0(proj, "outcomes_birth",date,".csv"), row.names = TRUE)
write.csv(outcomes_during_preg, file = paste0(proj, "outcomes_during_preg",date,".csv"), row.names = TRUE)
write.csv(outcomes_post_preg, file = paste0(proj, "outcomes_post_preg",date,".csv"), row.names = TRUE)
write.csv(vaccination, file = paste0(proj, "vaccination",date,".csv"), row.names = TRUE)
write.csv(skinny, file = paste0(proj, "skinny",date,".csv"), row.names = TRUE)
write.csv(covid, file = paste0(proj, "covid",date,".csv"), row.names = TRUE)



# 5. Creating the required variables --------------------------------------
head(cohort)
names(cohort) <- toupper(names(cohort))
ls(cohort)

cohort$PREG_START_DATE <- as.Date(cohort$PREG_START_DATE, "%d/%m/%Y") 
cohort$DELIVERY_DATE <- as.Date(cohort$DELIVERY_DATE, "%d/%m/%Y")
cohort$FU_END_DATE <- as.Date(cohort$FU_END_DATE, "%d/%m/%Y")

preg_start_est <-table(cohort$PREG_START_DATE_ESTIMATED)
cbind(preg_start_est,prop.table(preg_start_est))
rm("preg_start_est")

preg_start_corr <-table(cohort$PREG_START_DATE_CORRECTED)
cbind(preg_start_corr,prop.table(preg_start_corr))
rm("preg_start_corr")

summary(cohort$FU_DAYS)
dim(cohort)
cohort$FUP_PREG<-as.numeric(difftime(cohort$DELIVERY_DATE, cohort$PREG_START_DATE, unit="days"))
summary(cohort$FUP_PREG)

#short pregnancy follow-up
print(sort(cohort$FUP_PREG)[20])

cohort_skinny <- cohort %>% select(PERSON_ID,PREG_START_DATE)

####################
head(skinny)
ls(skinny)

skinny <- merge(cohort_skinny,skinny, by=c("PERSON_ID"), all=TRUE)

#check date of date before start of the pregnancy
table(is.na(skinny$DOD))

skinny<-skinny %>%
  rename(date_DOB = `_date_DOB`,
         date_DOD = `_date_DOD`,
         date_ETHNIC = `_date_ETHNIC`,
         date_LSOA = `_date_LSOA`,
         date_SEX = `_date_SEX`,
         source_DOB  = `_source_DOB`,
         source_DOD = `_source_DOD`,
         source_ETHNIC = `_source_ETHNIC`,
         source_LSOA = `_source_LSOA`,
         source_SEX = `_source_SEX`,
         tie_DOB = `_tie_DOB`,
         tie_DOD = `_tie_DOD`,
         tie_ETHNIC= `_tie_ETHNIC`,
         tie_LSOA = `_tie_LSOA`,
         tie_SEX = `_tie_SEX`)

skinny$DOB <- as.Date(skinny$DOB, "%d/%m/%Y")
skinny$COV_AGE<- as.integer((as.numeric(difftime(skinny$PREG_START_DATE, skinny$DOB, unit="days")))/365.25)
hist(skinny$COV_AGE)

summary(skinny$COV_AGE)

### Age group
agebreaks <- c(0, 30, 40, 500)
agelabels <- c("-30", "30-39", "+40")

skinny <- setDT(skinny)[ , agegroup := cut(COV_AGE, breaks = agebreaks, right = FALSE, labels = agelabels)]

agegroup <-table(skinny$agegroup)
cbind(agegroup,prop.table(agegroup))
rm("agegroup")

sex <-table(skinny$SEX)
cbind(sex,prop.table(sex))
rm("sex")

ethnicity <-table(skinny$ETHNIC_CAT)
cbind(ethnicity,prop.table(ethnicity))
rm("ethnicity")


skinny<-skinny %>%
  rename(COV_ETHNICITY = ETHNIC_CAT)

skinny <- skinny %>%
  mutate     (COV_ETHNICITY_3lev = case_when(skinny$COV_ETHNICITY == "Mixed" | skinny$COV_ETHNICITY == "Asian or Asian British" | skinny$COV_ETHNICITY == "Black or Black British"  ~  "Other",
                                             skinny$COV_ETHNICITY == "Unknown" |  skinny$COV_ETHNICITY == NA ~  "Unknown",
                                             skinny$COV_ETHNICITY == "White" ~  "White"
  )
  )

table(is.na(skinny$COV_ETHNICITY_3lev))
skinny$COV_ETHNICITY_3lev[is.na(skinny$COV_ETHNICITY_3lev)]="Unknown"
table(is.na(skinny$COV_ETHNICITY_3lev))

ethnicity_3lev <-table(skinny$COV_ETHNICITY_3lev)
cbind(ethnicity_3lev,prop.table(ethnicity_3lev))
rm("ethnicity_3lev")


skinny<-skinny %>%
  rename(COV_DEPRIVATION = IMD)

deprivation <-table(skinny$COV_DEPRIVATION)
cbind(deprivation,prop.table(deprivation))
rm("deprivation")


skinny <- skinny %>% select(-PREG_START_DATE)


################################################################################
###############               covariates                      ##################
################################################################################


covariates$cov_hx_diabetes_drugs_flag[is.na(covariates$cov_hx_diabetes_drugs_flag)] <- 0
covariates$cov_hx_diabetes_flag[is.na(covariates$cov_hx_diabetes_flag)] <- 0
covariates$cov_hx_hypertension_drugs_flag[is.na(covariates$cov_hx_hypertension_drugs_flag)] <- 0
covariates$cov_hx_hypertension_flag[is.na(covariates$cov_hx_hypertension_flag)] <- 0


table(covariates$cov_hx_diabetes_drugs_flag, covariates$cov_hx_diabetes_flag)
table(covariates$cov_hx_hypertension_drugs_flag, covariates$cov_hx_hypertension_flag)

covariates <- covariates %>%
  mutate     (COV_HX_HYPERTENSION = case_when(covariates$cov_hx_hypertension_drugs_flag == 0 & covariates$cov_hx_hypertension_flag==0  ~  0,
                                              covariates$cov_hx_hypertension_drugs_flag == 1 | covariates$cov_hx_hypertension_flag==1   ~ 1
  )
  )

covariates <- covariates %>%
  mutate     (COV_HX_DIABETES = case_when(covariates$cov_hx_diabetes_drugs_flag == 0 & covariates$cov_hx_diabetes_flag==0  ~  0,
                                          covariates$cov_hx_diabetes_drugs_flag == 1 | covariates$cov_hx_diabetes_flag==1   ~ 1
  )
  )

covariates$cov_hx_gest_diabetes_flag[is.na(covariates$cov_hx_gest_diabetes_flag)] <- 0
covariates$cov_hx_gest_hypertension_flag[is.na(covariates$cov_hx_gest_hypertension_flag)] <- 0
covariates$cov_hx_preeclampsia_flag[is.na(covariates$cov_hx_preeclampsia_flag)] <- 0
covariates$cov_hx_pregnancy_flag[is.na(covariates$cov_hx_pregnancy_flag)] <- 0
covariates$cov_hx_hypertension_pregnancy_flag[is.na(covariates$cov_hx_hypertension_pregnancy_flag)] <- 0

covariates <- covariates %>%
  mutate     (COV_HX_GEST_HYPERTENSION = case_when(covariates$cov_hx_hypertension_pregnancy_flag == 0 & covariates$cov_hx_gest_hypertension_flag==0  ~  0,
                                              covariates$cov_hx_hypertension_pregnancy_flag == 1 | covariates$cov_hx_gest_hypertension_flag==1   ~ 1
  )
  )

hx_pregnancy <-table(covariates$cov_hx_pregnancy_flag)
cbind(hx_pregnancy ,prop.table(hx_pregnancy))
rm("hx_pregnancy")

hx_preeclampsia <-table(covariates$cov_hx_preeclampsia_flag)
cbind(hx_preeclampsia ,prop.table(hx_preeclampsia))
rm("hx_preeclampsia")

# very low prevalence
hx_gest_hypertension <-table(covariates$cov_hx_hypertension_pregnancy_flag)
cbind(hx_gest_hypertension ,prop.table(hx_gest_hypertension))
rm("hx_gest_hypertension")

hx_gest_hypertension <-table(covariates$COV_HX_GEST_HYPERTENSION)
cbind(hx_gest_hypertension ,prop.table(hx_gest_hypertension))
rm("hx_gest_hypertension")


table(covariates$cov_hx_preeclampsia_flag, covariates$cov_hx_pregnancy_flag)
table(covariates$cov_hx_hypertension_pregnancy_flag, covariates$cov_hx_pregnancy_flag)

covariates$cov_hx_venous_flag[is.na(covariates$cov_hx_venous_flag)] <- 0
hx_venous <-table(covariates$cov_hx_venous_flag)
cbind(hx_venous ,prop.table(hx_venous))
rm("hx_venous")



covariates$cov_hx_pcos_flag[is.na(covariates$cov_hx_pcos_flag)] <- 0
hx_pcos <-table(covariates$cov_hx_pcos_flag)
cbind(hx_pcos ,prop.table(hx_pcos))
rm("hx_pcos")

covariates$cov_hx_ckd_flag[is.na(covariates$cov_hx_ckd_flag)] <- 0
hx_ckd <-table(covariates$cov_hx_ckd_flag)
cbind(hx_ckd ,prop.table(hx_ckd))
rm("hx_ckd")

covariates$cov_hx_prostate_cancer_flag[is.na(covariates$cov_hx_prostate_cancer_flag)] <- 0
hx_prostate_cancer <-table(covariates$cov_hx_prostate_cancer_flag)
cbind(hx_prostate_cancer ,prop.table(hx_prostate_cancer))
rm("hx_prostate_cancer")

covariates$cov_hx_covid19_flag[is.na(covariates$cov_hx_covid19_flag)] <- 0
hx_covid19 <-table(covariates$cov_hx_covid19_flag)
cbind(hx_covid19 ,prop.table(hx_covid19))
rm("hx_covid19")

covariates$cov_hx_bmi_obesity_flag[is.na(covariates$cov_hx_bmi_obesity_flag)] <- 0
hx_bmi_obesity <-table(covariates$cov_hx_bmi_obesity_flag)
cbind(hx_bmi_obesity ,prop.table(hx_bmi_obesity))
rm("hx_bmi_obesity")



covariates$cov_hx_depression_flag[is.na(covariates$cov_hx_depression_flag)] <- 0
hx_depression <-table(covariates$cov_hx_depression_flag)
cbind(hx_depression,prop.table(hx_depression))
rm("hx_depression")



covariates$cov_surg_last_yr_flag[is.na(covariates$cov_surg_last_yr_flag)] <- 0
hx_surg <-table(covariates$cov_surg_last_yr_flag)
cbind(hx_surg,prop.table(hx_surg))
rm("hx_surg")

covariates$cov_hx_anxiety_flag[is.na(covariates$cov_hx_anxiety_flag)] <- 0
hx_anxiety <-table(covariates$cov_hx_anxiety_flag)
cbind(hx_anxiety,prop.table(hx_anxiety))
rm("hx_anxiety")

covariates$cov_hx_dvt_flag[is.na(covariates$cov_hx_dvt_flag)] <- 0
hx_dvt <-table(covariates$cov_hx_dvt_flag)
cbind(hx_dvt,prop.table(hx_dvt))
rm("hx_dvt")

covariates$cov_hx_dvt_other_flag[is.na(covariates$cov_hx_dvt_other_flag)] <- 0
hx_dvt_oth <-table(covariates$cov_hx_dvt_other_flag)
cbind(hx_dvt_oth,prop.table(hx_dvt_oth))
rm("hx_dvt_oth")

covariates$cov_hx_dvt_pregnancy_flag[is.na(covariates$cov_hx_dvt_pregnancy_flag)] <- 0
hx_dvt_preg <-table(covariates$cov_hx_dvt_pregnancy_flag)
cbind(hx_dvt_preg,prop.table(hx_dvt_preg))
rm("hx_dvt_preg")

covariates$cov_hx_pe_flag[is.na(covariates$cov_hx_pe_flag)] <- 0
hx_pe<-table(covariates$cov_hx_pe_flag)
cbind(hx_pe,prop.table(hx_pe))
rm("hx_pe")


covariates$cov_hx_preterm_flag[is.na(covariates$cov_hx_preterm_flag)] <- 0
hx_preterm <-table(covariates$cov_hx_preterm_flag)
cbind(hx_preterm,prop.table(hx_preterm))
rm("hx_preterm")

covariates$cov_hx_stillbirth_flag[is.na(covariates$cov_hx_stillbirth_flag)] <- 0
hx_stillbirth <-table(covariates$cov_hx_stillbirth_flag)
cbind(hx_stillbirth,prop.table(hx_stillbirth))
rm("hx_stillbirth")

covariates$cov_hx_thrombophilia_flag[is.na(covariates$cov_hx_thrombophilia_flag)] <- 0
hx_thrombophilia <-table(covariates$cov_hx_thrombophilia_flag)
cbind(hx_thrombophilia,prop.table(hx_thrombophilia))
rm("hx_thrombophilia")

covariates$cov_meds_anticoagulant_flag[is.na(covariates$cov_meds_anticoagulant_flag)] <- 0
meds_anticoagulant <-table(covariates$cov_meds_anticoagulant_flag)
cbind(meds_anticoagulant,prop.table(meds_anticoagulant))
rm("meds_anticoagulant")


covariates$cov_meds_antiplatelet_flag[is.na(covariates$cov_meds_antiplatelet_flag)] <- 0
meds_antiplatelet <-table(covariates$cov_meds_antiplatelet_flag)
cbind(meds_antiplatelet,prop.table(meds_antiplatelet))
rm("meds_antiplatelet")

covariates$cov_meds_bp_lowering_flag[is.na(covariates$cov_meds_bp_lowering_flag)] <- 0
meds_bp_lowering <-table(covariates$cov_meds_bp_lowering_flag)
cbind(meds_bp_lowering,prop.table(meds_bp_lowering))
rm("meds_bp_lowering")


covariates$cov_meds_cocp_flag[is.na(covariates$cov_meds_cocp_flag)] <- 0
meds_cocp <-table(covariates$cov_meds_cocp_flag)
cbind(meds_cocp,prop.table(meds_cocp))
rm("meds_cocp")



covariates$cov_meds_hrt_flag[is.na(covariates$cov_meds_hrt_flag)] <- 0
meds_hrt <-table(covariates$cov_meds_hrt_flag)
cbind(meds_hrt,prop.table(meds_hrt))
rm("meds_hrt")

covariates$cov_meds_immunosuppressants_flag[is.na(covariates$cov_meds_immunosuppressants_flag)] <- 0
meds_immunosuppressants <-table(covariates$cov_meds_immunosuppressants_flag)
cbind(meds_immunosuppressants,prop.table(meds_immunosuppressants))
rm("meds_immunosuppressants")


covariates$cov_meds_lipid_lowering_flag[is.na(covariates$cov_meds_lipid_lowering_flag)] <- 0
meds_lipid_lowering <-table(covariates$cov_meds_lipid_lowering_flag)
cbind(meds_lipid_lowering,prop.table(meds_lipid_lowering))
rm("meds_lipid_lowering")



covariates <- covariates %>%
  mutate     (COV_HX_DIABETES_DIS = case_when(covariates$cov_hx_gest_diabetes_flag==0 & covariates$COV_HX_DIABETES==0 ~  0,
                                              covariates$cov_hx_gest_diabetes_flag==1 | covariates$COV_HX_DIABETES==1 ~  1
  )
  )


covariates <- covariates %>%
  mutate     (COV_HX_HYPERTENSIVE_DIS = case_when(covariates$cov_hx_gest_hypertension_flag==0 & covariates$cov_hx_preeclampsia_flag==0 & covariates$COV_HX_HYPERTENSION==0 ~  0,
                                                  covariates$cov_hx_gest_hypertension_flag==1 | covariates$cov_hx_preeclampsia_flag==1 | covariates$COV_HX_HYPERTENSION==1 ~  1
  )
  )


covariates <-covariates %>%
  rename(COV_HX_PREGNANCY_MAX = cov_hx_pregnancy_flag
         
  )


covariates <- covariates %>%
  mutate     (COV_HX_DEPR_ANX= case_when(covariates$cov_hx_anxiety_flag==0 & covariates$cov_hx_depression_flag==0 ~  0,
                                                  covariates$cov_hx_anxiety_flag==1 | covariates$cov_hx_depression_flag==1~  1
  )
  )

covariates <- covariates %>%
  mutate     (COV_HX_DVT_PE= case_when(covariates$cov_hx_dvt_flag==0 & covariates$cov_hx_dvt_other_flag==0 & covariates$cov_hx_dvt_pregnancy_flag==0 & covariates$cov_hx_pe_flag==0~  0,
                                         covariates$cov_hx_dvt_flag==1 | covariates$cov_hx_dvt_other_flag==1 | covariates$cov_hx_dvt_pregnancy_flag==1 | covariates$cov_hx_pe_flag==1~  1
  )
  )

covariates_selection <- covariates %>% select(PERSON_ID, cov_hx_stillbirth_flag, COV_HX_HYPERTENSIVE_DIS, COV_HX_DIABETES_DIS, cov_hx_preterm_flag,
                                              COV_HX_DEPR_ANX, COV_HX_DVT_PE, cov_hx_thrombophilia_flag, cov_hx_ckd_flag, cov_hx_covid19_flag, 
                                              cov_meds_anticoagulant_flag, cov_meds_antiplatelet_flag, cov_meds_bp_lowering_flag, cov_meds_lipid_lowering_flag,
                                              cov_meds_cocp_flag, cov_meds_hrt_flag, cov_hx_bmi_obesity_flag,cov_smoking_status,
                                              cov_hx_anxiety_flag, cov_hx_depression_flag,  COV_HX_PREGNANCY_MAX, 
                                               cov_hx_pcos_flag,  cov_surg_last_yr_flag,
                                               cov_hx_venous_flag, cov_meds_immunosuppressants_flag)
names(covariates_selection) <- toupper(names(covariates_selection))



####################


exposure <- exposure %>% 
  dplyr::mutate(exposure_vaccine1=ifelse(is.na(exposure_vaccine),0,exposure_vaccine),
                exposure_vacc_full1=ifelse(exposure_vaccine1==3,2,exposure_vaccine1),
                exposure_vacc_full=ifelse(exposure_vaccine==3,2,exposure_vaccine))



names(exposure) <- toupper(names(exposure))


exposure$FIRST_DOSE[is.na(exposure$FIRST_DOSE)] <- 0
first_vaccination <-table(exposure$FIRST_DOSE)
cbind(first_vaccination,prop.table(first_vaccination))
rm("first_vaccination")


### link with pregnancy period
cohort_exp <- cohort %>% select (PERSON_ID,  DELIVERY_DATE)
exposures <- merge(cohort_exp, exposure, by=c("PERSON_ID"), all= TRUE)


################################################################################
###############               outcomes                        ##################
################################################################################
 
 names(outcomes_birth) <- toupper(names(outcomes_birth))
 outcomes_birth$OUT_BIRTH_MULTI_GEST_MAX[is.na(outcomes_birth$OUT_BIRTH_MULTI_GEST_MAX)] <- 0


 outcomes_birth$OUT_BIRTH_STILLBIRTH_DELVAR[is.na(outcomes_birth$OUT_BIRTH_STILLBIRTH_DELVAR)] <- 0
 outcomes_birth$OUT_BIRTH_MULTI_GEST_DELCODE[is.na(outcomes_birth$OUT_BIRTH_MULTI_GEST_DELCODE)] <- 0
 outcomes_birth$OUT_BIRTH_STILLBIRTH_GENCODE_GDPPR[is.na(outcomes_birth$OUT_BIRTH_STILLBIRTH_GENCODE_GDPPR)] <- 0
 outcomes_birth$OUT_BIRTH_STILLBIRTH_GENCODE_HES[is.na(outcomes_birth$OUT_BIRTH_STILLBIRTH_GENCODE_HES)] <- 0

 table(outcomes_birth$OUT_BIRTH_STILLBIRTH_DELVAR, outcomes_birth$OUT_BIRTH_STILLBIRTH_GENCODE_GDPPR)
 table(outcomes_birth$OUT_BIRTH_MULTI_GEST_DELCODE, outcomes_birth$OUT_BIRTH_STILLBIRTH_GENCODE_HES)

 outcomes_birth$OUT_BIRTH_STILLBIRTH_MAX[is.na(outcomes_birth$OUT_BIRTH_STILLBIRTH_MAX)] <- 0


 still_delvar <-table(outcomes_birth$OUT_BIRTH_STILLBIRTH_DELCODE)
 cbind(still_delvar,prop.table(still_delvar))
 rm("still_delvar")

 still_hes <-table(outcomes_birth$OUT_BIRTH_STILLBIRTH_GENCODE_HES)
 cbind(still_hes,prop.table(still_hes))
 rm("still_hes")

 still_gp <-table(outcomes_birth$OUT_BIRTH_STILLBIRTH_GENCODE_GDPPR)
 cbind(still_gp,prop.table(still_gp))
 rm("still_gp")


 fup <- cohort %>% select(PERSON_ID,FUP_PREG)

 outcomes_birth <- merge(outcomes_birth,fup, by=c("PERSON_ID"))


 outcomes_birth <- outcomes_birth %>%
   mutate     (OUT_BIRTH_STILLBIRTH_MAX = case_when(outcomes_birth$OUT_BIRTH_STILLBIRTH_MAX==0  ~  0,
                                                    outcomes_birth$OUT_BIRTH_STILLBIRTH_MAX==1 & outcomes_birth$FUP_PREG < 240 ~  0,
                                                    outcomes_birth$OUT_BIRTH_STILLBIRTH_MAX==1 & outcomes_birth$FUP_PREG >= 240 ~  1
   )
   )



 outcomes_birth$OUT_BIRTH_IND_LAB[is.na(outcomes_birth$OUT_BIRTH_IND_LAB)] <- 0
 sum(is.na(outcomes_birth$OUT_BIRTH_IND_LAB))
 ind_lab <-table(outcomes_birth$OUT_BIRTH_IND_LAB)
 cbind(ind_lab,prop.table(ind_lab))
rm("ind_lab")
 
 table(outcomes_birth$OUT_BIRTH_PRETERM, outcomes_birth$OUT_BIRTH_MULTI_GEST_MAX)

 sum(is.na(outcomes_birth$OUT_BIRTH_PRETERM))
 outcomes_birth$OUT_BIRTH_PRETERM[is.na(outcomes_birth$OUT_BIRTH_PRETERM)] <- 0
 preterm <-table(outcomes_birth$OUT_BIRTH_PRETERM)
 cbind(preterm,prop.table(preterm))
 rm("preterm")

 sum(is.na(outcomes_birth$OUT_BIRTH_VERY_PRETERM))
 outcomes_birth$OUT_BIRTH_VERY_PRETERM[is.na(outcomes_birth$OUT_BIRTH_VERY_PRETERM)] <- 0
 verypreterm <-table(outcomes_birth$OUT_BIRTH_VERY_PRETERM)
 cbind(verypreterm,prop.table(verypreterm))
 rm("verypreterm")

 sum(is.na(outcomes_birth$OUT_BIRTH_EXT_PRETERM))
 outcomes_birth$OUT_BIRTH_EXT_PRETERM[is.na(outcomes_birth$OUT_BIRTH_EXT_PRETERM)] <- 0
 extpreterm <-table(outcomes_birth$OUT_BIRTH_EXT_PRETERM)
 cbind(extpreterm,prop.table(extpreterm))
 rm("extpreterm")

 sum(is.na(outcomes_birth$OUT_BIRTH_RESUS))
 outcomes_birth$OUT_BIRTH_RESUS[is.na(outcomes_birth$OUT_BIRTH_RESUS)] <- 0
 birth_resus <-table(outcomes_birth$OUT_BIRTH_RESUS)
 cbind(birth_resus,prop.table(birth_resus))
 rm("birth_resus")

 outcomes_birth$OUT_BIRTH_SMALL_GEST_AGE[is.na(outcomes_birth$OUT_BIRTH_SMALL_GEST_AGE)] <- 0
 small_gest_age <-table(outcomes_birth$OUT_BIRTH_SMALL_GEST_AGE)
 cbind(small_gest_age,prop.table(small_gest_age))
 rm("small_gest_age")

################################################################################
################################################################################

#### outcomes ####

names(outcomes_during_preg) <- toupper(names(outcomes_during_preg))


outcomes_during_preg$OUT_DUR_GEST_HYPERTENSION_FLAG[is.na(outcomes_during_preg$OUT_DUR_GEST_HYPERTENSION_FLAG)] <- 0 
gest_hyper <-table(outcomes_during_preg$OUT_DUR_GEST_HYPERTENSION_FLAG)
cbind(gest_hyper,prop.table(gest_hyper))
rm("gest_hyper")

outcomes_during_preg$OUT_DUR_PREECLAMPSIA_FLAG[is.na(outcomes_during_preg$OUT_DUR_PREECLAMPSIA_FLAG)] <- 0 
preeclampsia <-table(outcomes_during_preg$OUT_DUR_PREECLAMPSIA_FLAG)
cbind(preeclampsia,prop.table(preeclampsia))
rm("preeclampsia")

table (outcomes_during_preg$OUT_DUR_GEST_HYPERTENSION_FLAG, outcomes_during_preg$OUT_DUR_PREECLAMPSIA_FLAG)

outcomes_during_preg$OUT_DUR_GEST_DIABETES_FLAG[is.na(outcomes_during_preg$OUT_DUR_GEST_DIABETES_FLAG)] <- 0 
gest_diab <-table(outcomes_during_preg$OUT_DUR_GEST_DIABETES_FLAG)
cbind(gest_diab,prop.table(gest_diab))
rm("gest_diab")



outcomes_during_preg$OUT_DUR_VENOUS_FLAG[is.na(outcomes_during_preg$OUT_DUR_VENOUS_FLAG)] <- 0 
out_dur_venous <-table(outcomes_during_preg$OUT_DUR_VENOUS_FLAG)
cbind(out_dur_venous,prop.table(out_dur_venous))
rm("out_dur_venous")



################################################################################

names(outcomes_post_preg) <- toupper(names(outcomes_post_preg))

outcomes_post_preg$OUT_POST_VENOUS_FLAG[is.na(outcomes_post_preg$OUT_POST_VENOUS_FLAG)] <- 0 
out_post_venous <-table(outcomes_post_preg$OUT_POST_VENOUS_FLAG)
cbind(out_post_venous,prop.table(out_post_venous))
rm("out_post_venous")

table(outcomes_during_preg$OUT_DUR_VENOUS_FLAG, outcomes_post_preg$OUT_POST_VENOUS_FLAG)


out_dur_preg <- outcomes_during_preg %>% select(PERSON_ID,
                                                
                                                OUT_DUR_VENOUS_FLAG,  OUT_DUR_VENOUS_DATE,
                                               
                                                OUT_DUR_GEST_DIABETES_FLAG, OUT_DUR_GEST_DIABETES_DATE,
                                                OUT_DUR_PREECLAMPSIA_FLAG,  OUT_DUR_PREECLAMPSIA_DATE,
                                                OUT_DUR_GEST_HYPERTENSION_FLAG,  OUT_DUR_GEST_HYPERTENSION_DATE) 

out_dur_preg<-out_dur_preg %>%
  rename(
         OUT_BIN_DUR_VENOUS = OUT_DUR_VENOUS_FLAG,
         
         OUT_BIN_GEST_DIABETES = OUT_DUR_GEST_DIABETES_FLAG,
         OUT_BIN_PREECLAMPSIA  = OUT_DUR_PREECLAMPSIA_FLAG,
         OUT_BIN_GEST_HYPERTENSION  = OUT_DUR_GEST_HYPERTENSION_FLAG)


out_post_preg <- outcomes_post_preg %>% select(PERSON_ID,  
                                               
                                               OUT_POST_VENOUS_FLAG,  OUT_POST_VENOUS_DATE)

out_post_preg<-out_post_preg %>%
  rename(
         OUT_BIN_POST_VENOUS = OUT_POST_VENOUS_FLAG
         )


ids <- outcomes_birth %>% select(1)
out_birth <- outcomes_birth %>% select(OUT_BIRTH_PRETERM,OUT_BIRTH_STILLBIRTH_MAX, OUT_BIRTH_MULTI_GEST_MAX,
                                       OUT_BIRTH_VERY_PRETERM,  OUT_BIRTH_EXT_PRETERM,
                                       OUT_BIRTH_RESUS, OUT_BIRTH_SMALL_GEST_AGE)

out_birth<-out_birth %>%
  mutate(across(.names = 'OUT_BIN_{sub("OUT_", "", .col)}'))
out_birth <- out_birth %>% select(starts_with("OUT_BIN"))
out_birth <- cbind(ids, out_birth)
rm("ids")



# 6. Combining the dataset to form the analysis dataset ------------------


#################### combine dataframes ########################

data_temp1 <- merge(cohort,skinny, by=c("PERSON_ID"), all=TRUE)
exposures <- subset(exposure, select = -c(PREG_START_DATE, DELIVERY_DATE), all=TRUE)
data_temp2 <- merge(data_temp1,exposures, by=c("PERSON_ID"), all=TRUE)
outcome1 <- merge(out_birth, out_dur_preg, by=c("PERSON_ID"), all=TRUE)
outcomes <- merge(outcome1, out_post_preg, by=c("PERSON_ID"), all=TRUE)
data_temp3 <- merge(data_temp2,outcomes, by=c("PERSON_ID"), all=TRUE)
data_temp4 <- merge(data_temp3,covariates_selection, by=c("PERSON_ID"), all=TRUE)
rm("data_temp1","data_temp2","data_temp3")



##############
##### inclusion criteria CONSORT Diagram ############
ls(data_temp4)
data_temp4_1 <- data_temp4[which(data_temp4$FUP_PREG>=84),] # follow up period at least 84 days
data_temp4_4 <- data_temp4[which(data_temp4$FUP_PREG<84),]
data_temp4_2 <- data_temp4[which(data_temp4_1$COV_AGE>=45),]
data_temp4_3 <- data_temp4[which(data_temp4_1$COV_AGE<18),]
data_temp4_1 <- data_temp4_1[which(data_temp4_1$COV_AGE>=18 & data_temp4_1$COV_AGE<45),] # age criteria



data_temp4_1 <- data_temp4_1 %>% 
  dplyr::filter(!is.na(COV_DEPRIVATION)) # missing IMD index

data_temp4_1 <- data_temp4_1 %>% 
  dplyr::filter(is.na(COV_HX_HYSTERECTOMY_FLAG)) # History of hysterectomy


#write.csv(data_temp4_1, file = 'ccu036_01_data_temp_20220808.csv', row.names = TRUE)
write.csv(data_temp4_1, file = paste0(proj, "analysis_data_",date,".csv"), row.names = TRUE)



# updating the JCVI groups 04 sept 2023 (SDE)-----------------------------------

data <- dbGetQuery(con,'SELECT * FROM dsa_391419_j3w9t_collab.ccu036_01_cohort_jcvi')

data_final <- read_cs(paste0("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/data/ccu036_01_analysis_data_",date,".csv"))

d2 <- data %>% 
  dplyr::select(PERSON_ID, JCVI_GROUP_MIN)

df <- merge(data_final, d2, by.x = c("ID"), by.y = c("PERSON_ID"), all.x= TRUE)

setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/data")

proj <- 'ccu036_01_'
date <- '04092023' 



table(df$EXPOSURE_VACCINE_R)

data <- df %>%
  dplyr::filter(EXPOSURE_VACCINE_R!=4) %>%
  dplyr::mutate(vacc_covid_r=ifelse(EXPOSURE_VACCINE_R==0,0,1))%>%
  dplyr::mutate(VACC_1ST_DUR_DATE=ifelse(vacc_covid_r==1,dose1_dt,NA ))

write.csv(df, file = paste0(proj, "analysis_data_",date,".csv"), row.names = TRUE)

