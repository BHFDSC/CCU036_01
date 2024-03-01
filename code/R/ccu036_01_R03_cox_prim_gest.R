#===============================================================================
#### COX MODEL SCRIPT FOR CCU036_01 ########
#===============================================================================
#1. Load packages
#2. Load data
#3. Check and confirm the format the date and binary covariates
#4. Assign exposure variable for primary and sensitivity analysis 
#5. Define the cox_mode_full
#6. Specify the parameters
#7. Compile and save the estimate output file 


# input data -------------------------------------------------------------

### "ccu036_01_analysis_data_04092023.csv"



# output data -------------------------------------------------------------

## "out_dur_HRs_full_period.csv" 

#=============================================================================== 

###############################    packages   ################################

#install.packages("ggplot2")
#install.packages("tidyverse")
#install.packages("epiR")
#install.packages("scales")
#install.packages("dplyr")
#install.packages("survival")
#install.packages("survminer")
#install.packages("gtsummary")
#install.packages("reshape2")
#install.packages("date")
#install.packages("lubridate")
#install.packages("splines")

library(stringr)
library(ggplot2)
library(tidyverse)
library(scales)
library(dplyr)
library(survival)
library(survminer)
library(gtsummary)
library(reshape2)
library(lubridate)
library(splines)
library(purrr)

rm(list=ls())
rm("data", "out")



data<- read_csv("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/data/ccu036_01_analysis_data_04092023.csv")



################## selection criteria ########################

names(data) <- toupper(colnames(data))
table(data$PREG_START_DATE_ESTIMATED)

nrow(data)

#################### gestational week, outcomes during pregnancy  ##############
data$PREG_START_DATE <- as.Date(data$PREG_START_DATE)
data$OUT_DUR_PREECLAMPSIA_DATE <- as.Date(data$OUT_DUR_PREECLAMPSIA_DATE)
data$OUT_DUR_GEST_DIABETES_DATE <- as.Date(data$OUT_DUR_GEST_DIABETES_DATE)
data$OUT_DUR_GEST_HYPERTENSION_DATE <- as.Date(data$OUT_DUR_GEST_HYPERTENSION_DATE)
data$OUT_DUR_VENOUS_DATE <- as.Date(data$OUT_DUR_VENOUS_DATE)
data$EXP_DUR_COVID_1ST_DATE <- as.Date(data$EXP_DUR_COVID_1ST_DATE)



################## define date ########################


data$PREG_START_DATE <- as.Date(data$PREG_START_DATE) 
data$DELIVERY_DATE <- as.Date(data$DELIVERY_DATE)



data$OUT_BIRTH_PRETERM_RECORD_DATE <-  as.Date(data$DELIVERY_DATE)
data$OUT_BIRTH_VERY_PRETERM_RECORD_DATE <-  as.Date(data$DELIVERY_DATE)  
data$OUT_BIRTH_EXT_PRETERM_RECORD_DATE <-  as.Date(data$DELIVERY_DATE)  
data$OUT_BIRTH_SMALL_GEST_AGE_RECORD_DATE <-  as.Date(data$DELIVERY_DATE)  
data$OUT_BIRTH_STILLBIRTH_RECORD_DATE <-  as.Date(data$DELIVERY_DATE) 
data$OUT_BIRTH_MULTIGEST_RECORD_DATE <-  as.Date(data$DELIVERY_DATE) 

data$OUT_BIRTH_PRETERM_RECORD_DATE[data$OUT_BIN_BIRTH_PRETERM==0]<-  NA
data$OUT_BIRTH_VERY_PRETERM_RECORD_DATE[data$OUT_BIN_BIRTH_VERY_PRETERM==0]<-  NA 
data$OUT_BIRTH_EXT_PRETERM_RECORD_DATE[data$OUT_BIN_BIRTH_EXT_PRETERM==0]<-  NA
data$OUT_BIRTH_SMALL_GEST_AGE_RECORD_DATE[data$OUT_BIN_BIRTH_SMALL_GEST_AGE==0]<- NA
data$OUT_BIRTH_STILLBIRTH_RECORD_DATE[data$OUT_BIN_BIRTH_STILLBIRTH_MAX==0]<-  NA 
data$OUT_BIRTH_MULTIGEST_RECORD_DATE[data$OUT_BIN_BIRTH_MULTI_GEST_MAX==0]<-  NA


data$OUT_DUR_GEST_HYPERTENSION_RECORD_DATE <- as.Date(data$OUT_DUR_GEST_HYPERTENSION_DATE)
data$OUT_DUR_GEST_DIABETES_RECORD_DATE <- as.Date(data$OUT_DUR_GEST_DIABETES_DATE)
data$OUT_DUR_PREECLAMPSIA_RECORD_DATE <- as.Date(data$OUT_DUR_PREECLAMPSIA_DATE)
data$OUT_DUR_VENOUS_RECORD_DATE <- as.Date(data$OUT_DUR_VENOUS_DATE)



data$OUT_DUR_GEST_DIABETES_RECORD_DATE <- as.Date(data$OUT_DUR_GEST_DIABETES_RECORD_DATE, origin='1970-01-01')
data$OUT_DUR_GEST_HYPERTENSION_RECORD_DATE <- as.Date(data$OUT_DUR_GEST_HYPERTENSION_RECORD_DATE, origin='1970-01-01')
data$OUT_DUR_PREECLAMPSIA_RECORD_DATE <- as.Date(data$OUT_DUR_PREECLAMPSIA_RECORD_DATE, origin='1970-01-01')
data$OUT_DUR_VENOUS_RECORD_DATE <- as.Date(data$OUT_DUR_VENOUS_RECORD_DATE , origin='1970-01-01')
data$OUT_BIRTH_PRETERM_RECORD_DATE <- as.Date(data$OUT_BIRTH_PRETERM_RECORD_DATE, origin='1970-01-01')
data$OUT_BIRTH_VERY_PRETERM_RECORD_DATE <- as.Date(data$OUT_BIRTH_VERY_PRETERM_RECORD_DATE, origin='1970-01-01')
data$OUT_BIRTH_EXT_PRETERM_RECORD_DATE <- as.Date(data$OUT_BIRTH_EXT_PRETERM_RECORD_DATE, origin='1970-01-01')
data$OUT_BIRTH_SMALL_GEST_AGE_RECORD_DATE <- as.Date(data$OUT_BIRTH_SMALL_GEST_AGE_RECORD_DATE, origin='1970-01-01')
data$OUT_BIRTH_STILLBIRTH_RECORD_DATE <- as.Date(data$OUT_BIRTH_STILLBIRTH_RECORD_DATE , origin='1970-01-01')
data$OUT_BIRTH_MULTIGEST_RECORD_DATE <- as.Date(data$OUT_BIRTH_MULTIGEST_RECORD_DATE, origin='1970-01-01')


data$COV_HX_BMI_OBESITY_FLAG[is.na(data$COV_HX_BMI_OBESITY_FLAG)] <- 0
data$COV_HX_COVID19_FLAG[is.na(data$COV_HX_COVID19_FLAG)] <- 0
data$COV_HX_DEPR_ANX[is.na(data$COV_HX_DEPR_ANX)] <- 0
data$COV_HX_DIABETES_DIS[is.na(data$COV_HX_DIABETES_DIS)] <- 0
data$COV_HX_HYPERTENSIVE_DIS[is.na(data$COV_HX_HYPERTENSIVE_DIS)] <- 0
data$COV_HX_PCOS_FLAG[is.na(data$COV_HX_PCOS_FLAG)] <- 0
data$COV_HX_PREGNANCY_MAX[is.na(data$COV_HX_PREGNANCY_MAX)] <- 0
data$COV_HX_STILLBIRTH_FLAG[is.na(data$COV_HX_STILLBIRTH_FLAG)] <- 0
data$COV_HX_VENOUS_FLAG[is.na(data$COV_HX_VENOUS_FLAG)] <- 0
data$COV_MEDS_ANTIPLATELET_FLAG[is.na(data$COV_MEDS_ANTIPLATELET_FLAG)] <- 0
data$COV_MEDS_COCP_FLAG[is.na(data$COV_MEDS_COCP_FLAG)] <- 0
#data$COV_SMOKING_STATUS[is.na(data$COV_SMOKING_STATUS)] <- 0
data$COV_SURG_LAST_YR_FLAG[is.na(data$COV_SURG_LAST_YR_FLAG)] <- 0
names(data) <- tolower(colnames(data))
###########creating exposure variable for sensitivity analysis##################

#5. ######1. first vaccine before pregnancy - vacc_covid_r############################
data <- data %>%
  dplyr::filter(exposure_vaccine_r!=4) %>%
  dplyr::mutate(vacc_covid_r=ifelse(exposure_vaccine_r==0,0,1))%>%
  dplyr::mutate(VACC_1ST_DUR_DATE=ifelse(vacc_covid_r==1,dose1_dt,NA ))

data$VACC_1ST_DUR_DATE<- as.Date(data$VACC_1ST_DUR_DATE, origin='1970-01-01')
data$vacc_covid_date_r <- data$VACC_1ST_DUR_DATE


################## calendar period variable ###################
data$preg_start_date <- as.Date(data$preg_start_date) 
date_start <- ymd("2020/12/08")
data$calendar_month <- ((year(data$preg_start_date) - year(date_start)) * 12) + month(data$preg_start_date) - month(date_start)

date <- '06_09_2023' #(vacc_covid_r)# 

######################## run separated Cox models for all outcomes ##########################

writeLines(c("event model adj exp estimate conf.low conf.high stad.error n.event subgroup"), paste0('D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/',date,'/out_dur_HRs_full_period.csv'))

###NEED TO WORK FROM THIS
cox_model_full <- function(k, y, z) {
  
  ################# prepare for data transform to long format ########################
  data$fup_start <- data$preg_start_date+84
  
  #need to add each individual outcome to definition of overall_event below
  data$overall_event<- data[[k]]

  
  
  
  #need to add each individual outcome to definition of overall_event below
  data$overall_pregn_event_date<-data[[y]]
  
  ################# prepare for data transform to long format ########################
  
  
  if (z == "preterm" | z == "small_gest_age" | z == "stillbirth"|z=="venous"){
    
    data <- data[which(data$preg_start_date_estimated==0),]
    data <- data[which(data$out_bin_birth_multi_gest_max==0),]
    
  }

  
  #need to add each individual outcome to definition of overall_event below
  data$overall_pregn_event_date<-data[[y]]
  
  

  data$overall_pregn_event_date <- as.Date(data$overall_pregn_event_date, "%d/%m/%Y")

  data$fup_start <- as.Date(data$fup_start, "%d/%m/%Y")
  data$delivery_date <- as.Date(data$delivery_date, "%d/%m/%Y")
  data <- data %>%
    dplyr::mutate(fup_end=case_when(data$overall_event == 1 ~  data$overall_pregn_event_date,
                                  
                                    data$overall_event == 0  ~ data$delivery_date
    )
    )
  
  
  data$fup_end <- as.Date(data$fup_end, origin='1970-01-01')
  data$fup<-as.numeric(difftime(data$fup_end, data$fup_start, unit="days"))
  data$fup <- ifelse(data$fup ==0,  data$fup + 0.001, data$fup)

  
  
 
  data$overall_event_time<-as.numeric(difftime(data$overall_pregn_event_date, data$fup_start, unit="days"))
  data$age_sq <- data$cov_age^2
  
  
  ############## selection of the exposure and date of exposure#################
  data$vacc_covid <- data$vacc_covid_r  
  
  data <- data %>% 
    dplyr::filter(!is.na(vacc_covid))
  
   data$VACC_1ST_DUR_DATE<- as.Date(data$VACC_1ST_DUR_DATE, origin='1970-01-01')
   data$vacc_covid_date <- data$VACC_1ST_DUR_DATE

  
################  remove the rows with fup days less than 0#####################
   data <- data %>% 
     dplyr::filter(fup>0)
  ############       split rows in those who were infected and 
  ###############    those who were not infected    ##############################
  
  vacc_cases <- data[which(data$vacc_covid==1),]
  vacc_cases$delta_vacc_covid<-as.numeric(difftime(vacc_cases$vacc_covid_date, vacc_cases$preg_start_date, unit="days"))
  vacc_cases$delta_vacc_covid
  
  ################ variable list for long format transformation    ###############
  vars<-c("id",
          "calendar_month",
          "cov_age",
          "age_sq",
          "cov_deprivation",
          "preg_start_date",
          "delivery_date",
          "fup_start",
          "fup_end",
          "agegroup", 
          "region",
          "parity_label", 
          "dep_label",
          "cov_ethnicity_3lev",
          "dep_covid_pre",
          "vacc_covid",
          "dep_multi",
          "cov_hx_bmi_obesity_flag",
          "cov_hx_covid19_flag",
          "cov_hx_depr_anx",
          "cov_hx_diabetes_dis",
          "cov_hx_hypertensive_dis",
          "cov_hx_dvt_pe",
          "cov_hx_pcos_flag",
          "cov_hx_pregnancy_max",
          "cov_hx_stillbirth_flag",
          "cov_hx_venous_flag",
          "cov_hx_thrombophilia_flag",
          "cov_hx_ckd_flag",
          "cov_meds_lipid_lowering_flag",
          "cov_meds_anticoagulant_flag",
          "cov_meds_antiplatelet_flag",
          "cov_meds_cocp_flag",
          "cov_smoking_status",
          "cov_surg_last_yr_flag","agegp","jcvi_group_min"
  )
  
  ##################  data transform to long format for becoming infected #######################
  
  td_data <-
    tmerge(
      data1 = vacc_cases %>% select(all_of(vars), vacc_covid_date, overall_event_time),
      data2 = vacc_cases %>% select(all_of(vars), overall_event, fup,delta_vacc_covid),
      id = id,
      overall_event = event(fup,  overall_event)#,
     # vacc_covid = tdc(delta_vacc_covid)
    )
  

  
  without_expo <- data[which(data$vacc_covid==0),]
  without_expo$tstart<- c(0)
  without_expo$tstop <- ifelse(without_expo$fup ==0,  without_expo$fup + 0.001, without_expo$fup) # right now this isn't doing anything because I excluded those with 0 fup.
  without_expo$vacc_covid<- c(0)
  without_expo$last_step <- c(1)
  without_expo$hospitalised <- c(1)
  without_expo_noncases <- without_expo[which(without_expo$overall_event==0),]
  noncase_ids <- unique(without_expo_noncases$id)
  
  ###########  combine uninfected and infected exposure dataframes    ##################
  
  with_expo_cols <- colnames(td_data)
  with_expo_cols
  without_expo <- without_expo %>% dplyr::select(all_of(with_expo_cols))
  data_surv <-rbind(td_data, without_expo)
  rm(list=c("td_data", "without_expo"))
  
  #########################   final database    ################################
  survival_data <- data_surv
  
  
  event_model <- c(paste0(z))
  adjustment <-  c("non_adjusted", "adjusted",  "full_adjusted")
  analysis <-  c("full")
  
  cov <-  c("vacc_covid")
  
  cov_model <-  c("vacc_covid",
                  "vacc_covid + cov_age  +  age_sq ",
                  "vacc_covid + cov_age  +  age_sq  +  cov_deprivation  + cov_ethnicity_3lev + cov_hx_bmi_obesity_flag + cov_hx_covid19_flag + cov_hx_depr_anx + cov_hx_diabetes_dis + cov_hx_hypertensive_dis + cov_hx_pcos_flag + cov_hx_pregnancy_max + cov_hx_stillbirth_flag + cov_hx_venous_flag + region + cov_hx_thrombophilia_flag + cov_hx_ckd_flag + cov_meds_lipid_lowering_flag + cov_meds_anticoagulant_flag + cov_meds_antiplatelet_flag + cov_meds_cocp_flag + cov_smoking_status + cov_surg_last_yr_flag  + cov_hx_dvt_pe + jcvi_group_min + bs(calendar_month)")
  
  
  map2(cov_model, adjustment, function(x,i) {
    f <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  x))
    model <- coxph(f, survival_data, id = id)
    model$call$formula <- f
    s <- summary(model)
    k <- s$coefficients[1,,drop=F]
    cat(paste0(substring(event_model,1,10),' ',analysis,' ', i,' ',cov, apply(k, 1,
                                                                              function(x) {
                                                                                paste0(" ", round(exp(x[1]), 3),
                                                                                       ' ', round(exp(x[1] - 1.96 * x[3]), 3),
                                                                                       ' ', round(exp(x[1] + 1.96 * x[3]), 3),
                                                                                       " ", round((x[3]), 4),
                                                                                       " ", summary(model)$nevent)}),
               collapse = '\n'), '\n', sep = '', file = paste0("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/",date,"/out_dur_HRs_full_period.csv"), append = TRUE)
    invisible(model)
  })

}


cox_model_full("out_dur_venous", "out_dur_venous_record_date", "venous")
# cox_model_full("out_bin_birth_preterm", "out_birth_preterm_record_date", "preterm")
# cox_model_full("out_bin_birth_small_gest_age", "out_birth_small_gest_age_record_date", "small_gest_age")
# cox_model_full("out_bin_birth_stillbirth_max", "out_birth_stillbirth_record_date", "stillbirth")



######################## consider only models with more than 10 events ##########################

library(readr)
out <- read_table2(file = paste0("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/",date,"/out_dur_HRs_full_period.csv"))
nrow(out)
out <- out[which(out$n.event > 10),]
nrow(out)
data.table::fwrite(out,file = paste0("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/",date,"/out_dur_HRs_full_period.csv"))



