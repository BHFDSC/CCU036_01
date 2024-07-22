#===============================================================================
#### COX MODEL SCRIPT FOR CCU036_01 ########
#===============================================================================
#  ADDNL DESCRIPTION
#===============================================================================
# This is the additional script file to implement the revision. In this the 
# objectives are the following
# 1. adapt the script for changing the follow up start from 12 weeks to 24 weeks 
# for the preterm, small-for-gestational-age, adn stillbirth. 
# 2. check if the assumption for proportional hazard is satisfied or not 
# 3. if not adapt and finalise the model
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

library(reshape2)
library(lubridate)
library(splines)

library(readr)



rm(list = setdiff(ls(), c("DATABRICKS_GUID")))


data<- read_csv("~/ccu036_01_analysis_data_04092023.csv")


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
#6.########1a. type of first vaccine - vacc_az_r#####################################
table(data$dose1_name[data$vacc_covid_r==1])


data$vacc_az_r <- NA
data$vacc_az_r[data$vacc_covid_r==1 & data$dose1_name=="AstraZeneca"]  <- 1
data$vacc_az_r[data$vacc_covid_r==0] <- 0


#7.########1b. type of first vaccine - vacc_pf_r#####################################

data$vacc_pf_r <- NA
data$vacc_pf_r[data$vacc_covid_r==1 & data$dose1_name=="Pfizer"]  <- 1
data$vacc_pf_r[data$vacc_covid_r==0] <- 0

#16.#####11. first dose before start of pregnancy and second dose before 24 weeks of pregnancy: vacc2d_24wk #### ###### 


data <- data %>% 
  dplyr::mutate(wk_24 = as.Date(preg_start_date+168))
data$vacc2d_24wk <- NA
data$vacc2d_24wk[data$vacc_covid_r==1 & data$dose2_dt<=data$wk_24] <- 1
data$vacc2d_24wk[data$exposure_vaccine_r==0] <- 0


#17.#####12. first dose before start of pregnancy and third dose before 24 weeks of pregnancy: vacc3d_24wk #### ###### 


data <- data %>% 
  dplyr::mutate(wk_24 = as.Date(preg_start_date+168))
data$vacc3d_24wk <- NA
data$vacc3d_24wk[data$vacc_covid_r==1 & data$dose3_dt<=data$wk_24] <- 1
data$vacc3d_24wk[data$exposure_vaccine_r==0] <- 0


data$ethcat[data$cov_ethnicity=="Asian or Asian British"] <- 1
data$ethcat[data$cov_ethnicity=="Black or Black British"] <- 2
data$ethcat[data$cov_ethnicity=="Mixed"] <- 3
data$ethcat[data$cov_ethnicity=="Other"] <- 4
data$ethcat[data$cov_ethnicity=="Unknown"] <- 5
data$ethcat[data$cov_ethnicity=="White"] <- 6


################## calendar period variable ###################
data$preg_start_date <- as.Date(data$preg_start_date) 
date_start <- ymd("2020/12/08")
data$calendar_month <- ((year(data$preg_start_date) - year(date_start)) * 12) + month(data$preg_start_date) - month(date_start)

date <- '01_05_2024' #(vacc_covid_r)# ' #"01_09_2023":(vacc_covid_r), '02_09_2023' :(vacc_az_r), '03_09_2023': (vacc_pf_r) ,  '04_09_2023' :(vacc2d_24wk), '05_09_2023': (vacc3d_24wk) 

data1 <- data
# 1. VENOUS EVENT ##########--------------------------------------------------
  
  data <- data1
  data$fup_start <- data$preg_start_date+84 
  
  data$overall_event<- data$out_dur_venous
  
  table(data$overall_event)
  
  
  data$overall_pregn_event_date<-data$out_dur_venous_record_date
  
  table(data$overall_event)
  
  data$overall_pregn_event_date<-data$out_dur_venous_record_date
  
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
  
  ############### stratification ###########################
  
  data$parity_label <-ifelse(data$cov_hx_pregnancy_max==1, "hx_of_preg", "no_hx_of_preg")
  data$parity_label <- factor(data$parity_label, levels = c("hx_of_preg", "no_hx_of_preg"))
  data$parity_label = relevel(data$parity_label, ref = "no_hx_of_preg")
  
  data <- data %>%
    mutate(COV_DEP_BIN = case_when(data$cov_deprivation<= 2 ~ 0,
                                   data$cov_deprivation >= 3 ~  1
    )
    )
  
  data$dep_label <- ifelse(data$COV_DEP_BIN==1, "high_dep", "low_dep")
  data$dep_label <- factor(data$dep_label, levels = c( "high_dep", "low_dep"))
  data$dep_label = relevel(data$dep_label, ref = "low_dep")
  
  data$dep_covid_pre <- ifelse(data$cov_hx_covid19_flag==1, "yes", "no")
  data$dep_covid_pre <- factor(data$dep_covid_pre, levels = c( "yes", "no"))
  data$dep_covid_pre = relevel(data$dep_covid_pre, ref = "no")
  
  data$dep_multi <- ifelse(data$out_bin_birth_multi_gest_max==1, "multiple", "singleton")
  data$dep_multi <- factor(data$dep_multi, levels = c( "multiple", "singleton"))
  data$dep_multi = relevel(data$dep_multi, ref = "singleton")
  
  data$eth_new <- ifelse(data$ethcat==1, "Asian", ifelse(data$ethcat==2, "Black", "Non AB"))
  data$eth_new <- factor(data$eth_new, levels = c( "Asian", "Black", "Non AB"))
  
  data$jcvi <- ifelse(data$jcvi_group_min==4, "grp_4", ifelse(data$jcvi_group_min==10, "grp_10", ifelse(data$jcvi_group_min==11, "grp_11","grp_12")))
  data$jcvi <- factor(data$jcvi, levels = c( "grp_4", "grp_10", "grp_11", "grp_12"))
  
  
  ############## seelction of the exposure and date of exposure#################
  data$vacc_covid <- data$vacc_covid_r  #"30_04_2024":(vacc_covid_r); "01_09_2023":(vacc_covid_r), '13_11_2023' :(vacc_az_r), '03_09_2023': (vacc_pf_r) ,  '04_09_2023' :(vacc2d_24wk), '05_09_2023': (vacc3d_24wk) 
  
  data <- data %>% 
    dplyr::filter(!is.na(vacc_covid))
  
  data$VACC_1ST_DUR_DATE<- as.Date(data$VACC_1ST_DUR_DATE, origin='1970-01-01')
  data$vacc_covid_date <- data$VACC_1ST_DUR_DATE
  summary(data$fup)
  
  ################  remove the rows with fup days less than 0#####################
  data <- data %>% 
    dplyr::filter(fup>0) #%>% 
    # dplyr::mutate(fup=ifelse(fup>91,91,fup))
  ############       split rows in those who were infected and 
  ###############    those who were not infected    ##############################
  summary(data$fup)
  vacc_cases <- data[which(data$vacc_covid==1),]
  vacc_cases$delta_vacc_covid<-as.numeric(difftime(vacc_cases$vacc_covid_date, vacc_cases$preg_start_date, unit="days"))
  # vacc_cases$delta_vacc_covid
  
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
  rm(list=c("td_data", "without_expo", "vacc_cases","without_expo_noncases"))
  
  # write.csv(data_surv,"data_surv_vte.csv")

  #########################   final database    ################################
  survival_data <- data_surv
  
########## VENOUS EVENTS
  cov_model2 <-  c("vacc_covid")
  cov_model4 <-  c("vacc_covid + cov_age") 
  cov_model5 <-  c("vacc_covid + cov_age  +  as.factor(cov_deprivation)  + cov_ethnicity_3lev + cov_hx_bmi_obesity_flag + cov_hx_covid19_flag + cov_hx_depr_anx + cov_hx_diabetes_dis + cov_hx_hypertensive_dis + cov_hx_pcos_flag + cov_hx_pregnancy_max + cov_hx_stillbirth_flag + cov_hx_venous_flag + region + cov_hx_thrombophilia_flag + cov_hx_ckd_flag  + cov_meds_anticoagulant_flag + cov_meds_antiplatelet_flag + cov_meds_cocp_flag + cov_smoking_status + cov_surg_last_yr_flag   + as.factor(jcvi_group_min) + bs(calendar_month)")
  cov_model6 <-  c("vacc_covid + cov_age  +  as.factor(cov_deprivation)  + cov_ethnicity_3lev + cov_hx_bmi_obesity_flag + cov_hx_covid19_flag + cov_hx_depr_anx + cov_hx_diabetes_dis + cov_hx_hypertensive_dis + cov_hx_pcos_flag + cov_hx_pregnancy_max + cov_hx_stillbirth_flag + cov_hx_venous_flag + region + cov_hx_thrombophilia_flag + cov_hx_ckd_flag  + cov_meds_anticoagulant_flag + cov_meds_antiplatelet_flag + cov_meds_cocp_flag + cov_smoking_status + cov_surg_last_yr_flag   + as.factor(jcvi_group_min) + bs(calendar_month, degree=1, knots=c(0.33, 0.66))")
  
  
  # map2(cov_model, adjustment, function(x,i) {
    f2 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model2))
    f4 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model4))
    f5 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model5))
    f6 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model6))
    model2 <- coxph(f2, survival_data, id = id)
    model4 <- coxph(f4, survival_data, id = id)
    model5 <- coxph(f5, survival_data, id = id)
    model56<- coxph(f6, survival_data, id = id)
    
    model2
    model4
    model5
    model56
    
    head(model.matrix(model5))

    
    survival_data <- survival_data %>% 
      mutate(deprv2  = model.matrix(model5)[, "as.factor(cov_deprivation)2"],
             deprv3  = model.matrix(model5)[, "as.factor(cov_deprivation)3"],
             deprv4  = model.matrix(model5)[, "as.factor(cov_deprivation)4"],
             deprv5 = model.matrix(model5)[, "as.factor(cov_deprivation)5"],
             eth_unkwn         = model.matrix(model5)[, "cov_ethnicity_3levUnknown"],
             eth_wht         = model.matrix(model5)[, "cov_ethnicity_3levWhite"],
             reg_east_eng         = model.matrix(model5)[, "regionEast of England"],
             reg_lond         = model.matrix(model5)[, "regionLondon"],
             reg_NE         = model.matrix(model5)[, "regionNorth East"],
             reg_NW         = model.matrix(model5)[, "regionNorth West"],
             reg_SE         = model.matrix(model5)[, "regionSouth East"],
             reg_SW         = model.matrix(model5)[, "regionSouth West"],
             reg_WM         = model.matrix(model5)[, "regionWest Midlands"],
             reg_YH         = model.matrix(model5)[, "regionYorkshire and The Humber"],
             smk_curr         = model.matrix(model5)[, "cov_smoking_statusCurrent"],
             smk_ex         = model.matrix(model5)[, "cov_smoking_statusEx"],
             smk_nev         = model.matrix(model5)[, "cov_smoking_statusNever"],
             jcvi10         = model.matrix(model5)[, "as.factor(jcvi_group_min)10"],
             jcvi11         = model.matrix(model5)[, "as.factor(jcvi_group_min)11"],
             jcvi12         = model.matrix(model5)[, "as.factor(jcvi_group_min)12"],
             cm1         = model.matrix(model5)[, "bs(calendar_month)1"],
             cm2         = model.matrix(model5)[, "bs(calendar_month)2"],
             cm3         = model.matrix(model5)[, "bs(calendar_month)3"])
    
    cov_model.dummy <-  c("vacc_covid + cov_age  + deprv2 + deprv3 + deprv4 + deprv5 + eth_unkwn + eth_wht  + reg_east_eng + reg_lond + reg_NE + reg_NW + reg_SE + reg_SW + reg_WM + reg_YH  + cov_hx_bmi_obesity_flag + cov_hx_covid19_flag + cov_hx_depr_anx + cov_hx_diabetes_dis + cov_hx_hypertensive_dis + cov_hx_pcos_flag + cov_hx_pregnancy_max + cov_hx_stillbirth_flag + cov_hx_venous_flag + cov_hx_thrombophilia_flag + cov_hx_ckd_flag + cov_meds_anticoagulant_flag + cov_meds_antiplatelet_flag + cov_meds_cocp_flag + smk_curr + smk_ex + smk_nev + jcvi10 + jcvi11 + jcvi12 + cov_surg_last_yr_flag   +  cm1 + cm2 + cm3")#+ cov_meds_lipid_lowering_flag
    
    
   
    f <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model.dummy))
    model.dummy <- coxph(f, survival_data, id = id)
    
    model.dummy
  
  model5  
    

  test.ph <- cox.zph(model.dummy)
    summary(test.ph5)
    
    
    setwd("~/collab/CCU036_01/results/01_05_2024")
   library(survminer)
    png("Fig.ven.Schnfld.png",  units="in", width=20, height=20, pointsize=12, res=300)
    
   ggcoxzph(test.ph)
    
    dev.off()
    
   library(ggplot2)
    
    
  cov_model.tt <-  c("vacc_covid + cov_age   + deprv2 + deprv3 + deprv4 + deprv5 + eth_unkwn + eth_wht  + reg_east_eng + reg_lond + reg_NE + reg_NW + reg_SE + reg_SW + reg_WM + reg_YH  + cov_hx_bmi_obesity_flag + cov_hx_covid19_flag + cov_hx_depr_anx + cov_hx_diabetes_dis + cov_hx_hypertensive_dis + cov_hx_pcos_flag + cov_hx_pregnancy_max + cov_hx_stillbirth_flag + cov_hx_venous_flag + cov_hx_thrombophilia_flag + cov_hx_ckd_flag + cov_meds_anticoagulant_flag + cov_meds_antiplatelet_flag + cov_meds_cocp_flag + smk_curr + smk_ex + smk_nev + jcvi10 + jcvi11 + jcvi12 + cov_surg_last_yr_flag   +  cm1 + cm2 + cm3 + tt(deprv3) + tt(cov_hx_hypertensive_dis) + tt(cov_meds_cocp_flag)")#+ cov_meds_lipid_lowering_flag
    f <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model.tt))
    model.tt <- coxph(f, survival_data, id = id,tt = function(x,t,...) x*t ) ## FINAL MODEL FOR VENOUS EVENTS
    
  model.tt
    
  
  cov_model.tt
  
  
  anv <- car::Anova(model.tt, type = 3, test = "Wald")
  
  write.csv(anv, "ven_anova.csv")
  
  class(test.ph)
  class(anv)
  library(broom)
  ven_ph <- test.ph$table
  
  write.csv(ven_ph, "ven_ph.csv")
  
# AHR plot
  
  survival_data <- survival_data %>% 
    dplyr::mutate(fup= tstop-tstart)
  SUB    <- survival_data$overall_event == 1
  TIME   <- seq(min(survival_data$fup[SUB]),
                max(survival_data$fup[SUB]), 1)
  BETA1   <- coef(model.tt)["deprv3"]
  BETATT1 <- coef(model.tt)["tt(deprv3)"]
  AHR1    <- exp(BETA1 + BETATT1*TIME)
  
  
  

  BETA2   <- coef(model.tt)["cov_hx_hypertensive_dis"]
  BETATT2 <- coef(model.tt)["tt(cov_hx_hypertensive_dis)"]
  AHR2    <- exp(BETA2 + BETATT2*TIME)
  
  

  BETA3   <- coef(model.tt)["cov_meds_cocp_flag"]
  BETATT3 <- coef(model.tt)["tt(cov_meds_cocp_flag)"]
  AHR3    <- exp(BETA3 + BETATT3*TIME)
  
  
  
  png("Fig.ven.AHR.png",  units="in", width=12, height=6, pointsize=25, res=300)
  
  par(mfrow=c(1,3))
  plot(TIME, AHR1, type = "l", main="Deprivation Q3")
  abline(h = 1, lty = 2, col = "darkgray")
  abline(v = -1*BETA1/BETATT1, lty = 2, col = "darkgray")
  plot(TIME, AHR2, type = "l", main="H/o Hypertensive disease")
  abline(h = 1, lty = 2, col = "darkgray")
  abline(v = -1*BETA2/BETATT2, lty = 2, col = "darkgray")
  
  plot(TIME, AHR3, type = "l", main="H/o COCP")
  abline(h = 1, lty = 2, col = "darkgray")
  abline(v = -1*BETA3/BETATT3, lty = 2, col = "darkgray")
  
  mtext("AHR for non-proprotional covariates - Venous events model ", side = 3, outer = TRUE, line = -1.5, cex = 1)
  
  dev.off()
  
  
  
  # proptionality plot
  
  png("Fig.ven.PH_part1.png",  units="in", width=14, height=12, pointsize=15, res=300)
  par(mfrow=c(3,5))
  plot(test.ph[1],ylab="Vaccine exposure", main ="p.value = 0.117" )
  plot(test.ph[2],ylab="Age", main ="p.value = 0.680" )
  plot(test.ph[3],ylab="Deprivation Q2", main ="p.value = 0.435" )
  plot(test.ph[4],ylab="Deprivation Q3", main ="p.value = 0.025" )
  plot(test.ph[5],ylab="Deprivation Q4", main ="p.value = 0.621" )
  plot(test.ph[6],ylab="Deprivation Q5", main ="p.value = 0.819" )
  plot(test.ph[7],ylab="Ethnicity Unknown", main ="p.value = 0.737" )
  plot(test.ph[8],ylab="Ethnicity White", main ="p.value = 0.188" )
  plot(test.ph[9],ylab="Region East England", main ="p.value = 0.234" )
  plot(test.ph[10],ylab="Region London", main ="p.value = 0.505" )
  plot(test.ph[11],ylab="Region NorthEast", main ="p.value = 0.981" )
  plot(test.ph[12],ylab="Region NorthWes", main ="p.value = 0.329" )
  plot(test.ph[13],ylab="Region SouthEast", main ="p.value = 0.398" )
  plot(test.ph[14],ylab="Region SouthWest", main ="p.value = 0.063" )
  plot(test.ph[15],ylab="Region WestMidlands", main ="p.value = 0.314" )
  mtext("Venous events Schoenfeld test (global p value 0.068) - I", side = 3, outer = TRUE, line = -1.5, cex = 1)
  dev.off()
  
  
  png("Fig.ven.PH_part2.png",  units="in", width=14, height=12, pointsize=15, res=300)
  par(mfrow=c(3,5))
  plot(test.ph[16],ylab="Region York", main ="p.value = 0.292" )
  plot(test.ph[17],ylab="H/o Obesity", main ="p.value = 0.599" )
  plot(test.ph[18],ylab="H/o COVID-19", main ="p.value = 0.197" )
  plot(test.ph[19],ylab="H/o Depression anxiety", main ="p.value = 0.528" )
  plot(test.ph[20],ylab="H/o Diabetes", main ="p.value = 0.598" )
  plot(test.ph[21],ylab="H/o Hypertensive disease", main ="p.value = 0.043" )
  plot(test.ph[22],ylab="H/o PCOS", main ="p.value = 0.152" )
  plot(test.ph[23],ylab="H/o Pregnancy", main ="p.value = 0.246" )
  plot(test.ph[24],ylab="H/o Stillbirth", main ="p.value = 0.184" )
  plot(test.ph[25],ylab="H/o Venous event", main ="p.value = 0.475" )
  plot(test.ph[26],ylab="H/o Thrombophiliat", main ="p.value = 0.870" )
  plot(test.ph[27],ylab="H/o Chronic kidney disease", main ="p.value = 0.380" )
  plot(test.ph[28],ylab="H/o anticoagulant", main ="p.value = 0.134" )
  plot(test.ph[29],ylab="H/o antiplatelet", main ="p.value = 0.875" )
  plot(test.ph[30],ylab="H/o COCP", main ="p.value = 0.033" )
  mtext("Venous events Schoenfeld test (global p value 0.068) - II", side = 3, outer = TRUE, line = -1.5, cex = 1)
  dev.off()
  
  png("Fig.ven.PH_part3.png",  units="in", width=14, height=10, pointsize=15, res=300)
  par(mfrow=c(2,5))
  plot(test.ph[31],ylab="Current smoker", main ="p.value = 0.311" )
  plot(test.ph[32],ylab="Ex smoker", main ="p.value = 0.257" )
  plot(test.ph[33],ylab="Never smoker", main ="p.value = 0.195" )
  plot(test.ph[34],ylab="JCVI 10", main ="p.value = 0.225" )
  plot(test.ph[35],ylab="JCVI 11", main ="p.value = 0.748" )
  plot(test.ph[36],ylab="JCVI 12", main ="p.value = 0.505" )
  plot(test.ph[37],ylab="Surgery in last year", main ="p.value = 0.131" )
  mtext("Venous events Schoenfeld test (global p value 0.068) - III", side = 3, outer = TRUE, line = -1.5, cex = 1)
  dev.off()
  

  

    
    
    # Linearity assumption
    # Residuals vs. continuous predictor
    X1 <- survival_data$cov_age
    Y1 <- resid(model.dummy, type = "martingale")
    setwd("~/collab/CCU036_01/results/01_05_2024")
    png("Fig.ven.linearity.png",  units="in", width=10, height=10, pointsize=25, res=300)
    plot(X1, Y1, pch = 20, col = "darkgray",
         xlab = "Mother's Age", ylab = "Martingale Residual",
         main = "Residuals vs. Predictor")
    abline(h = 0)
    lines(smooth.spline(X1, Y1, df = 7), lty = 2, lwd = 2)
    dev.off()
    
    # Influential observations
  setwd("~/collab/CCU036_01/results/01_05_2024")
  
  DFbetas <- resid(model.tt, type="dfbetas")
  DFbetas1 <- resid(model.dummy, type="dfbetas")
head(DFbetas1)
  
  # Plot
  dfbetas <- as.data.frame(DFbetas)
  a <- plot(DFbetas[,1], ylab = "Var.1")
  
  png("Fig.ven.dfb_part1.png",  units="in", width=14, height=12, pointsize=15, res=300)
  par(mfrow=c(3,5))
  plot(DFbetas1[, 1], ylab="Vaccine exposure", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 2], ylab="Age", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 3], ylab="Deprivation Q2", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 4], ylab="Deprivation Q3", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 5], ylab="Deprivation Q4", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 6], ylab="Deprivation Q5", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 7], ylab="Ethnicity Unknown", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 8], ylab="Ethnicity White", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 9], ylab="Region East England", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 10], ylab="Region London", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 11], ylab="Region NorthEast", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 12], ylab="Region NorthWest", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 13], ylab="Region SouthEast", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 14], ylab="Region SouthWest", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 15], ylab="Region WestMidlands", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  mtext("Venous events influential observation - part 1", side = 3, outer = TRUE, line = -2, cex = 1.5)
  dev.off()
  
  
  png("Fig.ven.dfb_part2.png",  units="in", width=14, height=12, pointsize=15, res=300)
  par(mfrow=c(3,5))
  plot(DFbetas1[, 16], ylab="Region York", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 17], ylab="H/o Obesity", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 18], ylab="H/o COVID-19", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 19], ylab="H/o Depression anxiety", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 20], ylab="H/o Diabetes", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 21], ylab="H/o Hypertensive disease", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 22], ylab="H/o PCOS", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 23], ylab="H/o Pregnancy", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 24], ylab="H/o Stillbirth", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 25], ylab="H/o Venous event", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 26], ylab="H/o Thrombophilia", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 27], ylab="H/o Chronic kidney disease", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 28], ylab="H/o anticoagulant", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 29], ylab="H/o antiplatelet", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 30], ylab="H/o COCP", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  mtext("Venous events influential observation - part 2", side = 3, outer = TRUE, line = -2, cex = 1.5)
  dev.off()
  
  
  png("Fig.ven.dfb_part3.png",  units="in", width=14, height=10, pointsize=15, res=300)
  par(mfrow=c(2,5))
  plot(DFbetas1[, 31], ylab="Current smoker", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 2], ylab="Ex smoker", ylim=c(-0.3,0.3))
  abline(h = c(-0.32, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 33], ylab="Never smoker", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 34], ylab="JCVI 10", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 35], ylab="JCVI 11", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 36], ylab="JCVI 12", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  plot(DFbetas1[, 37], ylab="Surgery in last year", ylim=c(-0.3,0.3))
  abline(h = c(-0.2, 0.2), lty = 2, col = "red")

  mtext("Venous events influential observation - part 3", side = 3, outer = TRUE, line = -2, cex = 1.5)
  dev.off()
  
  

    # 2. STILLBIRTH #########------------------------------
    
    data <- data1
    data$fup_start <- data$preg_start_date+168
    
    
    data$overall_event<- data$out_bin_birth_stillbirth_max 
    
    table(data$overall_event)
    
    
    
    #need to add each individual outcome to definition of overall_event below
    
    data$overall_pregn_event_date<-data$out_birth_stillbirth_record_date
    
    ################# prepare for data transform to long format ########################
    
    #need to add each individual outcome to definition of overall_event below
    table(data$overall_event)
    
    
    
    data$overall_pregn_event_date<-data$out_birth_stillbirth_record_date 
    
    
    
    
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
    
    ############### stratification ###########################
    
    
    data$parity_label <-ifelse(data$cov_hx_pregnancy_max==1, "hx_of_preg", "no_hx_of_preg")
    data$parity_label <- factor(data$parity_label, levels = c("hx_of_preg", "no_hx_of_preg"))
    data$parity_label = relevel(data$parity_label, ref = "no_hx_of_preg")
    
    
    
    data <- data %>%
      mutate(COV_DEP_BIN = case_when(data$cov_deprivation<= 2 ~ 0,
                                     data$cov_deprivation >= 3 ~  1
      )
      )
    
    data$dep_label <- ifelse(data$COV_DEP_BIN==1, "high_dep", "low_dep")
    data$dep_label <- factor(data$dep_label, levels = c( "high_dep", "low_dep"))
    data$dep_label = relevel(data$dep_label, ref = "low_dep")
    
    data$dep_covid_pre <- ifelse(data$cov_hx_covid19_flag==1, "yes", "no")
    data$dep_covid_pre <- factor(data$dep_covid_pre, levels = c( "yes", "no"))
    data$dep_covid_pre = relevel(data$dep_covid_pre, ref = "no")
    
    data$dep_multi <- ifelse(data$out_bin_birth_multi_gest_max==1, "multiple", "singleton")
    data$dep_multi <- factor(data$dep_multi, levels = c( "multiple", "singleton"))
    data$dep_multi = relevel(data$dep_multi, ref = "singleton")
    
    data$eth_new <- ifelse(data$ethcat==1, "Asian", ifelse(data$ethcat==2, "Black", "Non AB"))
    data$eth_new <- factor(data$eth_new, levels = c( "Asian", "Black", "Non AB"))
    
    data$jcvi <- ifelse(data$jcvi_group_min==4, "grp_4", ifelse(data$jcvi_group_min==10, "grp_10", ifelse(data$jcvi_group_min==11, "grp_11","grp_12")))
    data$jcvi <- factor(data$jcvi, levels = c( "grp_4", "grp_10", "grp_11", "grp_12"))
    
    
    ############## seelction of the exposure and date of exposure#################
    data$vacc_covid <- data$vacc_covid_r  #"30_04_2024":(vacc_covid_r); "01_09_2023":(vacc_covid_r), '13_11_2023' :(vacc_az_r), '03_09_2023': (vacc_pf_r) ,  '04_09_2023' :(vacc2d_24wk), '05_09_2023': (vacc3d_24wk) 
    
    data <- data %>% 
      dplyr::filter(!is.na(vacc_covid))
    
    data$VACC_1ST_DUR_DATE<- as.Date(data$VACC_1ST_DUR_DATE, origin='1970-01-01')
    data$vacc_covid_date <- data$VACC_1ST_DUR_DATE
    summary(data$fup)
    
    ################  remove the rows with fup days less than 0#####################
    data <- data %>% 
      dplyr::filter(fup>0) 
    ############       split rows in those who were exposed and 
    ###############    those who were not exposed    ##############################
    
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
    rm(list=c("td_data", "without_expo", "vacc_cases","without_expo_noncases"))
    
    
    
    #########################   final database    ################################
    survival_data <- data_surv
   
    cov_model2 <-  c("vacc_covid")
    cov_model4 <-  c("vacc_covid + cov_age")
    cov_model5 <-  c("vacc_covid + cov_age   +  as.factor(cov_deprivation)  + cov_ethnicity_3lev + cov_hx_bmi_obesity_flag + cov_hx_covid19_flag + cov_hx_depr_anx + cov_hx_diabetes_dis + cov_hx_hypertensive_dis + cov_hx_pcos_flag + cov_hx_pregnancy_max + cov_hx_stillbirth_flag + cov_hx_venous_flag + region + cov_hx_thrombophilia_flag + cov_hx_ckd_flag + cov_meds_lipid_lowering_flag +  cov_meds_antiplatelet_flag + cov_meds_cocp_flag + cov_smoking_status + cov_surg_last_yr_flag   + as.factor(jcvi_group_min) + bs(calendar_month)")#cov_meds_anticoagulant_flag +
    
    
    f2 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model2))
    f4 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model4))
    f5 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model5))
    
    model2 <- coxph(f2, survival_data, id = id)
    model4 <- coxph(f4, survival_data, id = id)
    model5 <- coxph(f5, survival_data, id = id)
    
    model2
    model4
    model5
    
    head(model.matrix(model5))
    
    
    survival_data <- survival_data %>% 
      mutate(deprv2  = model.matrix(model5)[, "as.factor(cov_deprivation)2"],
             deprv3  = model.matrix(model5)[, "as.factor(cov_deprivation)3"],
             deprv4  = model.matrix(model5)[, "as.factor(cov_deprivation)4"],
             deprv5 = model.matrix(model5)[, "as.factor(cov_deprivation)5"],
             eth_unkwn         = model.matrix(model5)[, "cov_ethnicity_3levUnknown"],
             eth_wht         = model.matrix(model5)[, "cov_ethnicity_3levWhite"],
             reg_east_eng         = model.matrix(model5)[, "regionEast of England"],
             reg_lond         = model.matrix(model5)[, "regionLondon"],
             reg_NE         = model.matrix(model5)[, "regionNorth East"],
             reg_NW         = model.matrix(model5)[, "regionNorth West"],
             reg_SE         = model.matrix(model5)[, "regionSouth East"],
             reg_SW         = model.matrix(model5)[, "regionSouth West"],
             reg_WM         = model.matrix(model5)[, "regionWest Midlands"],
             reg_YH         = model.matrix(model5)[, "regionYorkshire and The Humber"],
             smk_curr         = model.matrix(model5)[, "cov_smoking_statusCurrent"],
             smk_ex         = model.matrix(model5)[, "cov_smoking_statusEx"],
             smk_nev         = model.matrix(model5)[, "cov_smoking_statusNever"],
             jcvi10         = model.matrix(model5)[, "as.factor(jcvi_group_min)10"],
             jcvi11         = model.matrix(model5)[, "as.factor(jcvi_group_min)11"],
             jcvi12         = model.matrix(model5)[, "as.factor(jcvi_group_min)12"],
             cm1         = model.matrix(model5)[, "bs(calendar_month)1"],
             cm2         = model.matrix(model5)[, "bs(calendar_month)2"],
             cm3         = model.matrix(model5)[, "bs(calendar_month)3"])
    
    cov_model.dummy <-  c("vacc_covid + cov_age   + deprv2 + deprv3 + deprv4 + deprv5 + eth_unkwn + eth_wht  + reg_east_eng + reg_lond + reg_NE + reg_NW + reg_SE + reg_SW + reg_WM + reg_YH  + cov_hx_bmi_obesity_flag + cov_hx_covid19_flag + cov_hx_depr_anx + cov_hx_diabetes_dis + cov_hx_hypertensive_dis + cov_hx_pcos_flag + cov_hx_pregnancy_max + cov_hx_stillbirth_flag + cov_hx_venous_flag + cov_hx_thrombophilia_flag + cov_hx_ckd_flag + cov_meds_lipid_lowering_flag + cov_meds_antiplatelet_flag + cov_meds_cocp_flag + smk_curr + smk_ex + smk_nev + jcvi10 + jcvi11 + jcvi12 + cov_surg_last_yr_flag   + cm1 + cm2 + cm3")# cov_meds_anticoagulant_flag +
    
    f <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model.dummy))
    model.dummy <- coxph(f, survival_data, id = id) # FINAL MODEL FOR STILLBIRTH
    
    model.dummy
    
    
    model5

    
    test.ph <- cox.zph(model.dummy)
    test.ph
    setwd("~/collab/CCU036_01/results/01_05_2024")
    anv <- car::Anova(model.dummy, type = 3, test = "Wald")
    
    write.csv(anv, "sb_anova.csv")
    
    class(test.ph)
    class(anv)
    library(broom)
    sb_ph <- test.ph$table
    
    write.csv(sb_ph, "sb_ph.csv")
    
    
    # proptionality plot
    
    png("Fig.sb.PH_part1.png",  units="in", width=14, height=12, pointsize=15, res=300)
    par(mfrow=c(3,5))
    plot(test.ph[1],ylab="Vaccine exposure", main ="p.value = 0.243" )
    plot(test.ph[2],ylab="Age", main ="p.value = 0.803" )
    plot(test.ph[3],ylab="Deprivation Q2", main ="p.value = 0.367" )
    plot(test.ph[4],ylab="Deprivation Q3", main ="p.value = 0.992" )
    plot(test.ph[5],ylab="Deprivation Q4", main ="p.value = 0.875" )
    plot(test.ph[6],ylab="Deprivation Q5", main ="p.value = 0.955" )
    plot(test.ph[7],ylab="Ethnicity Unknown", main ="p.value = 0.359" )
    plot(test.ph[8],ylab="Ethnicity White", main ="p.value = 0.083" )
    plot(test.ph[9],ylab="Region East England", main ="p.value = 0.910" )
    plot(test.ph[10],ylab="Region London", main ="p.value = 0.575" )
    plot(test.ph[11],ylab="Region NorthEast", main ="p.value = 0.062" )
    plot(test.ph[12],ylab="Region NorthWes", main ="p.value = 0.305" )
    plot(test.ph[13],ylab="Region SouthEast", main ="p.value = 0.069" )
    plot(test.ph[14],ylab="Region SouthWest", main ="p.value = 0.846" )
    plot(test.ph[15],ylab="Region WestMidlands", main ="p.value = 0.135" )
    mtext("Stillbirth Schoenfeld test (global p value 0.279) - I", side = 3, outer = TRUE, line = -1.5, cex = 1)
    dev.off()
    
    
    png("Fig.sb.PH_part2.png",  units="in", width=14, height=12, pointsize=15, res=300)
    par(mfrow=c(3,5))
    plot(test.ph[16],ylab="Region York", main ="p.value = 0.518" )
    plot(test.ph[17],ylab="H/o Obesity", main ="p.value = 0.545" )
    plot(test.ph[18],ylab="H/o COVID-19", main ="p.value = 0.636" )
    plot(test.ph[19],ylab="H/o Depression anxiety", main ="p.value = 0.165" )
    plot(test.ph[20],ylab="H/o Diabetes", main ="p.value = 0.343" )
    plot(test.ph[21],ylab="H/o Hypertensive disease", main ="p.value = 0.233" )
    plot(test.ph[22],ylab="H/o PCOS", main ="p.value = 0.554" )
    plot(test.ph[23],ylab="H/o Pregnancy", main ="p.value = 0.637" )
    plot(test.ph[24],ylab="H/o Stillbirth", main ="p.value = 0.359" )
    plot(test.ph[25],ylab="H/o Venous event", main ="p.value = 0.432" )
    plot(test.ph[26],ylab="H/o Thrombophiliat", main ="p.value = 0.181" )
    plot(test.ph[27],ylab="H/o Chronic kidney disease", main ="p.value = 0.710" )
    plot(test.ph[28],ylab="H/o lipid lowering drugs", main ="p.value = 0.753" )
    plot(test.ph[29],ylab="H/o antiplatelet", main ="p.value = 0.269" )
    plot(test.ph[30],ylab="H/o COCP", main ="p.value = 0.428" )
    mtext("Stillbirth Schoenfeld test (global p value 0.279) - II", side = 3, outer = TRUE, line = -1.5, cex = 1)
    dev.off()
    
    png("Fig.sb.PH_part3.png",  units="in", width=14, height=10, pointsize=15, res=300)
    par(mfrow=c(2,5))
    plot(test.ph[31],ylab="Current smoker", main ="p.value = 0.838" )
    plot(test.ph[32],ylab="Ex smoker", main ="p.value = 0.423" )
    plot(test.ph[33],ylab="Never smoker", main ="p.value = 0.117" )
    plot(test.ph[34],ylab="JCVI 10", main ="p.value = 0.976" )
    plot(test.ph[35],ylab="JCVI 11", main ="p.value = 0.480" )
    plot(test.ph[36],ylab="JCVI 12", main ="p.value = 0.352" )
    plot(test.ph[37],ylab="Surgery in last year", main ="p.value = 0.315" )
    mtext("Stillbirth Schoenfeld test (global p value 0.279) - III", side = 3, outer = TRUE, line = -1.5, cex = 1)
    dev.off()
    
    
    
    
    
    
    # Linearity assumption
    # Residuals vs. continuous predictor
    X2 <- survival_data$cov_age
    Y2 <- resid(model.dummy, type = "martingale")
   
    png("Fig.sb.linearity.png",  units="in", width=10, height=10, pointsize=25, res=300)
    plot(X2, Y2, pch = 20, col = "darkgray",
         xlab = "Age", ylab = "Martingale Residual",
         main = "Residuals vs. Predictor")
    abline(h = 0)
    lines(smooth.spline(X2, Y2, df = 7), lty = 2, lwd = 2)
    dev.off()
    
    # Influential observations
    setwd("~/collab/CCU036_01/results/01_05_2024")
    
    # DFbetas <- resid(model.tt, type="dfbetas")
    DFbetas1 <- resid(model.dummy, type="dfbetas")
    head(DFbetas1)
    
    # Plot
  
    
    png("Fig.sb.dfb_part1.png",  units="in", width=14, height=12, pointsize=15, res=300)
    par(mfrow=c(3,5))
    plot(DFbetas1[, 1], ylab="Vaccine exposure", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 2], ylab="Age", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 3], ylab="Deprivation Q2", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 4], ylab="Deprivation Q3", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 5], ylab="Deprivation Q4", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 6], ylab="Deprivation Q5", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 7], ylab="Ethnicity Unknown", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 8], ylab="Ethnicity White", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 9], ylab="Region East England", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 10], ylab="Region London", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 11], ylab="Region NorthEast", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 12], ylab="Region NorthWest", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 13], ylab="Region SouthEast", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 14], ylab="Region SouthWest", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 15], ylab="Region WestMidlands", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    mtext("Stillbirth influential observation - I", side = 3, outer = TRUE, line = -2, cex = 1.5)
    dev.off()
    
    
    png("Fig.sb.dfb_part2.png",  units="in", width=14, height=12, pointsize=15, res=300)
    par(mfrow=c(3,5))
    plot(DFbetas1[, 16], ylab="Region York", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 17], ylab="H/o Obesity", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 18], ylab="H/o COVID-19", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 19], ylab="H/o Depression anxiety", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 20], ylab="H/o Diabetes", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 21], ylab="H/o Hypertensive disease", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 22], ylab="H/o PCOS", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 23], ylab="H/o Pregnancy", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 24], ylab="H/o Stillbirth", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 25], ylab="H/o Venous event", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 26], ylab="H/o Thrombophilia", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 27], ylab="H/o Chronic kidney disease", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 28], ylab="H/o lipid lowering drugs", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 29], ylab="H/o antiplatelet", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 30], ylab="H/o COCP", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    mtext("Stillbirth influential observation - II", side = 3, outer = TRUE, line = -2, cex = 1.5)
    dev.off()
    
    
    png("Fig.sb.dfb_part3.png",  units="in", width=14, height=10, pointsize=15, res=300)
    par(mfrow=c(2,5))
    plot(DFbetas1[, 31], ylab="Current smoker", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 2], ylab="Ex smoker", ylim=c(-0.3,0.3))
    abline(h = c(-0.32, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 33], ylab="Never smoker", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 34], ylab="JCVI 10", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 35], ylab="JCVI 11", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 36], ylab="JCVI 12", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    plot(DFbetas1[, 37], ylab="Surgery in last year", ylim=c(-0.3,0.3))
    abline(h = c(-0.2, 0.2), lty = 2, col = "red")
    
    mtext("Stillbirth influential observation - III", side = 3, outer = TRUE, line = -2, cex = 1.5)
    dev.off()
    
    
    
   
   # 3. PRETERM MODEL NEW -------------------------------------------------------------
   
    data <- data1
 
   
    data$fup_start <- data$preg_start_date+168 
   data$overall_event<- data$out_bin_birth_preterm 
   table(data$overall_event)
   
   data$overall_pregn_event_date<-data$out_birth_preterm_record_date 
   
   
   
   
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
   data$fup <- ifelse(data$fup>97, 97, data$fup)# censoring at 36 weeks for preterm
   summary(data$fup)
   
   
   
   data$overall_event_time<-as.numeric(difftime(data$overall_pregn_event_date, data$fup_start, unit="days"))
   data$age_sq <- data$cov_age^2
   
   ############### stratification ###########################
   
   
   data$parity_label <-ifelse(data$cov_hx_pregnancy_max==1, "hx_of_preg", "no_hx_of_preg")
   data$parity_label <- factor(data$parity_label, levels = c("hx_of_preg", "no_hx_of_preg"))
   data$parity_label = relevel(data$parity_label, ref = "no_hx_of_preg")
   
   
   
   data <- data %>%
     mutate(COV_DEP_BIN = case_when(data$cov_deprivation<= 2 ~ 0,
                                    data$cov_deprivation >= 3 ~  1
     )
     )
   
   data$dep_label <- ifelse(data$COV_DEP_BIN==1, "high_dep", "low_dep")
   data$dep_label <- factor(data$dep_label, levels = c( "high_dep", "low_dep"))
   data$dep_label = relevel(data$dep_label, ref = "low_dep")
   
   data$dep_covid_pre <- ifelse(data$cov_hx_covid19_flag==1, "yes", "no")
   data$dep_covid_pre <- factor(data$dep_covid_pre, levels = c( "yes", "no"))
   data$dep_covid_pre = relevel(data$dep_covid_pre, ref = "no")
   
   data$dep_multi <- ifelse(data$out_bin_birth_multi_gest_max==1, "multiple", "singleton")
   data$dep_multi <- factor(data$dep_multi, levels = c( "multiple", "singleton"))
   data$dep_multi = relevel(data$dep_multi, ref = "singleton")
   
   data$eth_new <- ifelse(data$ethcat==1, "Asian", ifelse(data$ethcat==2, "Black", "Non AB"))
   data$eth_new <- factor(data$eth_new, levels = c( "Asian", "Black", "Non AB"))
   
   data$jcvi <- ifelse(data$jcvi_group_min==4, "grp_4", ifelse(data$jcvi_group_min==10, "grp_10", ifelse(data$jcvi_group_min==11, "grp_11","grp_12")))
   data$jcvi <- factor(data$jcvi, levels = c( "grp_4", "grp_10", "grp_11", "grp_12"))
   
   
   ############## seelction of the exposure and date of exposure#################
   data$vacc_covid <- data$vacc_covid_r  #"30_04_2024":(vacc_covid_r); "01_09_2023":(vacc_covid_r), '13_11_2023' :(vacc_az_r), '03_09_2023': (vacc_pf_r) ,  '04_09_2023' :(vacc2d_24wk), '05_09_2023': (vacc3d_24wk) 
   
   data <- data %>% 
     dplyr::filter(!is.na(vacc_covid))
   
   data$VACC_1ST_DUR_DATE<- as.Date(data$VACC_1ST_DUR_DATE, origin='1970-01-01')
   data$vacc_covid_date <- data$VACC_1ST_DUR_DATE
   summary(data$fup)
   
   ################  remove the rows with fup days less than 0#####################
   data <- data %>% 
     dplyr::filter(fup>0) 
   
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
   
   
   td_data <-
     tmerge(
       data1 = data %>% select(all_of(vars), vacc_covid_date, overall_event_time),
       data2 = data %>% select(all_of(vars), overall_event, fup),
       id = id,
       overall_event = event(fup,  overall_event)#,
       # vacc_covid = tdc(delta_vacc_covid)
     )
   ##################  split long format dataframe into pre and post infection #####################
   

   td_data <- td_data %>% dplyr::rename(t0=tstart, t=tstop) %>% dplyr::mutate(tstart=0, tstop=t-t0)
   
   cuts_weeks_since_expo <- c(56)
   
   ###  data transform to long format for time from infection for those infected ####
   
   td_data<-survSplit(Surv(tstop, overall_event) ~., td_data,
                                 cut=cuts_weeks_since_expo,
                                
                                 episode ="Weeks_category")
   
   
   ###########  prepare dataframe for those  with infection exposure   ##################
   td_data <- td_data %>% dplyr::mutate(tstart=tstart+t0, tstop=tstop+t0) %>% dplyr::select(-c(t0,t))
   

   td_data  <- td_data %>%
     dplyr::group_by(id) %>% dplyr::arrange(Weeks_category) %>% dplyr::mutate(last_step = ifelse(row_number()==n(),1,0))
   td_data$overall_event  <- td_data$overall_event * td_data$last_step
   
   data_surv <- td_data
   
   # write.csv(data_surv,"data_surv_ptm.csv")

   survival_data <- data_surv 
   
   cov_model1 <- c("vacc_covid")
   cov_model2 <- c("vacc_covid:strata(Weeks_category)")
   cov_model3 <- c("vacc_covid*strata(Weeks_category)")
   cov_model4 <-c("vacc_covid:strata(Weeks_category) + cov_age")
   cov_model5 <-  c("vacc_covid:strata(Weeks_category) + cov_age  +  as.factor(cov_deprivation)  + cov_ethnicity_3lev + cov_hx_bmi_obesity_flag + cov_hx_covid19_flag + cov_hx_depr_anx + cov_hx_diabetes_dis + cov_hx_hypertensive_dis + cov_hx_pcos_flag + cov_hx_pregnancy_max + cov_hx_stillbirth_flag + cov_hx_venous_flag + region + cov_hx_thrombophilia_flag + cov_hx_ckd_flag + cov_meds_lipid_lowering_flag + cov_meds_anticoagulant_flag + cov_meds_antiplatelet_flag + cov_meds_cocp_flag + cov_smoking_status + cov_surg_last_yr_flag   + as.factor(jcvi_group_min) + bs(calendar_month)")
   
   f1 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model1))
   f2 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model2))
   f3 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model3))
   f4 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model4))
   f5 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model5))
   
   
   model1 <- coxph(f1, survival_data, id = id)
   model2 <- coxph(f2, survival_data, id = id)
   model3 <- coxph(f3, survival_data, id = id)
   model4 <- coxph(f4, survival_data, id = id)
   model5 <- coxph(f5, survival_data, id = id)
   
   model1
   model2
   model3
   model4
   model5
 
   
   test.ph1 <- cox.zph(model1)
   test.ph2 <- cox.zph(model2)
   test.ph3 <- cox.zph(model3)
   test.ph4 <- cox.zph(model4)
   test.ph5 <- cox.zph(model5)
   
   test.ph1
   test.ph2
   test.ph3
   test.ph4
   test.ph5
     plot(test.ph5)
   
   tail(model.matrix(model5))
   
   survival_data <- survival_data %>% 
     ungroup() %>% 
     mutate(deprv2  = model.matrix(model5)[, "as.factor(cov_deprivation)2"],
            deprv3  = model.matrix(model5)[, "as.factor(cov_deprivation)3"],
            deprv4  = model.matrix(model5)[, "as.factor(cov_deprivation)4"],
            deprv5 = model.matrix(model5)[, "as.factor(cov_deprivation)5"],
            eth_unkwn         = model.matrix(model5)[, "cov_ethnicity_3levUnknown"],
            eth_wht         = model.matrix(model5)[, "cov_ethnicity_3levWhite"],
            reg_east_eng         = model.matrix(model5)[, "regionEast of England"],
            reg_lond         = model.matrix(model5)[, "regionLondon"],
            reg_NE         = model.matrix(model5)[, "regionNorth East"],
            reg_NW         = model.matrix(model5)[, "regionNorth West"],
            reg_SE         = model.matrix(model5)[, "regionSouth East"],
            reg_SW         = model.matrix(model5)[, "regionSouth West"],
            reg_WM         = model.matrix(model5)[, "regionWest Midlands"],
            reg_YH         = model.matrix(model5)[, "regionYorkshire and The Humber"],
            smk_curr         = model.matrix(model5)[, "cov_smoking_statusCurrent"],
            smk_ex         = model.matrix(model5)[, "cov_smoking_statusEx"],
            smk_nev         = model.matrix(model5)[, "cov_smoking_statusNever"],
            jcvi10         = model.matrix(model5)[, "as.factor(jcvi_group_min)10"],
            jcvi11         = model.matrix(model5)[, "as.factor(jcvi_group_min)11"],
            jcvi12         = model.matrix(model5)[, "as.factor(jcvi_group_min)12"],
            cm1         = model.matrix(model5)[, "bs(calendar_month)1"],
            cm2         = model.matrix(model5)[, "bs(calendar_month)2"],
            cm3         = model.matrix(model5)[, "bs(calendar_month)3"],
            vacc1         = model.matrix(model5)[, "vacc_covid:strata(Weeks_category)Weeks_category=1"],
            vacc2         = model.matrix(model5)[, "vacc_covid:strata(Weeks_category)Weeks_category=2"])
   
   cov_model.dummy <-  c("cov_age   + deprv2 + deprv3 + deprv4 + deprv5 + eth_unkwn + eth_wht  + reg_east_eng + reg_lond + reg_NE + reg_NW + reg_SE + reg_SW + reg_WM + reg_YH  + cov_hx_bmi_obesity_flag + cov_hx_covid19_flag + cov_hx_depr_anx + cov_hx_diabetes_dis + cov_hx_hypertensive_dis + cov_hx_pcos_flag + cov_hx_pregnancy_max + cov_hx_stillbirth_flag + cov_hx_venous_flag + cov_hx_thrombophilia_flag + cov_hx_ckd_flag + cov_meds_lipid_lowering_flag + cov_meds_anticoagulant_flag   + cov_meds_antiplatelet_flag + cov_meds_cocp_flag + smk_curr + smk_ex + smk_nev + jcvi10 + jcvi11 + jcvi12 + cov_surg_last_yr_flag   +  cm1  + cm2 + cm3 + vacc1 + vacc2")
   f <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model.dummy))
   model.dummy <- coxph(f, survival_data, id = id)
   
   model.dummy
   
   model5
   
   test.ph <-  cox.zph(model.dummy)
   summary(test.ph)
   summary(test.phsga$time)
 str(test.ph)
 str(test.phsga)
 
 library(splines)
 body(ns)  
 test.phptm <- na.omit(test.ph)
   cov_model.tt <-  c("cov_age + deprv2 + deprv3 + deprv4 + deprv5 + eth_unkwn + eth_wht  + reg_east_eng + reg_lond + reg_NE + reg_NW + reg_SE + reg_SW + reg_WM + reg_YH  + cov_hx_bmi_obesity_flag + cov_hx_covid19_flag + cov_hx_depr_anx + cov_hx_diabetes_dis + cov_hx_hypertensive_dis + cov_hx_pcos_flag + cov_hx_pregnancy_max + cov_hx_stillbirth_flag + cov_hx_venous_flag + cov_hx_thrombophilia_flag + cov_hx_ckd_flag + cov_meds_lipid_lowering_flag + cov_meds_anticoagulant_flag   + cov_meds_antiplatelet_flag + cov_meds_cocp_flag + smk_curr + smk_ex + smk_nev + jcvi10 + jcvi11 + jcvi12 + cov_surg_last_yr_flag   +   cm1 +  cm2 + cm3  + tt(vacc1) + tt(deprv2) + tt(deprv3) + tt(deprv4) + tt(eth_wht) + tt(reg_lond) + tt(cov_hx_bmi_obesity_flag) + tt(cov_hx_covid19_flag) +tt(cov_hx_diabetes_dis) +tt(cov_hx_pregnancy_max) + tt(cov_hx_stillbirth_flag) +tt(cov_hx_venous_flag) +tt(cov_meds_anticoagulant_flag) + tt(smk_ex) + tt(jcvi11) +tt(cm2) +tt(cm3) + vacc1 + vacc2")
   f <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model.tt))
   model.tt <- coxph(f, survival_data, id = id,tt = function(x,t,...) x*t ) ## FINAL MODEL FOR PRETERM 
   
   model.tt
   
   plot(test.ph, resid=TRUE, se=TRUE, df=4, nsmo=40, var,
        xlab="Time", ylab, lty=1:2, col=1, lwd=1, hr=FALSE, plot=TRUE)
   
   plot(test.phsga, var=2)
   test.ph[2]
   plot.cox.zph(ptm)
   
   
   


   
   png("Fig.ptm.E1.png",  units="in", width=10, height=10, pointsize=25, res=300)
   
   setwd("~/collab/CCU036_01/results/01_05_2024")
   anv <- car::Anova(model.tt, type = 3, test = "Wald")
   
   write.csv(anv, "ptm_anova.csv")
   
   class(test.ph)
   class(anv)
   library(broom)
   ptm_ph <- test.ph$table
   
   write.csv(ptm_ph, "ptm_ph.csv")
   
   # AHR plot
   
   survival_data <- survival_data %>% 
     dplyr::mutate(fup= tstop-tstart)
   SUB    <- survival_data$overall_event == 1
   TIME   <- seq(min(survival_data$fup[SUB]),
                 max(survival_data$fup[SUB]), 1)
   TIME1   <- seq(0,
                 70, 1)
   BETA1   <- coef(model.tt)["vacc1"]
   BETATT1 <- coef(model.tt)["tt(vacc1)"]
   AHR1    <- exp(BETA1 + BETATT1*TIME1)
   

   BETA2   <- coef(model.tt)["deprv2"]
   BETATT2 <- coef(model.tt)["tt(deprv2)"]
   AHR2    <- exp(BETA2 + BETATT2*TIME)

   BETA3   <- coef(model.tt)["deprv3"]
   BETATT3 <- coef(model.tt)["tt(deprv3)"]
   AHR3    <- exp(BETA3 + BETATT3*TIME)

   BETA4   <- coef(model.tt)["deprv4"]
   BETATT4 <- coef(model.tt)["tt(deprv4)"]
   AHR4    <- exp(BETA4 + BETATT4*TIME)
   
   BETA5   <- coef(model.tt)["eth_wht"]
   BETATT5 <- coef(model.tt)["tt(eth_wht)"]
   AHR5    <- exp(BETA5 + BETATT5*TIME)
   
   BETA6   <- coef(model.tt)["reg_lond"]
   BETATT6 <- coef(model.tt)["tt(reg_lond)"]
   AHR6    <- exp(BETA6 + BETATT6*TIME)
   
   BETA7   <- coef(model.tt)["cov_hx_bmi_obesity_flag"]
   BETATT7 <- coef(model.tt)["tt(cov_hx_bmi_obesity_flag)"]
   AHR7    <- exp(BETA7 + BETATT7*TIME)
   
   BETA8   <- coef(model.tt)["cov_hx_covid19_flag"]
   BETATT8 <- coef(model.tt)["tt(cov_hx_covid19_flag)"]
   AHR8    <- exp(BETA8 + BETATT8*TIME)
   
   BETA9   <- coef(model.tt)["cov_hx_diabetes_dis"]
   BETATT9 <- coef(model.tt)["tt(cov_hx_diabetes_dis)"]
   AHR9    <- exp(BETA9 + BETATT9*TIME)
   
   BETA10   <- coef(model.tt)["cov_hx_pregnancy_max"]
   BETATT10 <- coef(model.tt)["tt(cov_hx_pregnancy_max)"]
   AHR10    <- exp(BETA10 + BETATT10*TIME)
   
   BETA11   <- coef(model.tt)["cov_hx_venous_flag"]
   BETATT11 <- coef(model.tt)["tt(cov_hx_venous_flag)"]
   AHR11    <- exp(BETA11 + BETATT11*TIME)

   BETA12   <- coef(model.tt)["cov_meds_anticoagulant_flag"]
   BETATT12 <- coef(model.tt)["tt(cov_meds_anticoagulant_flag)"]
   AHR12    <- exp(BETA12 + BETATT12*TIME)
   
   BETA13   <- coef(model.tt)["smk_ex"]
   BETATT13 <- coef(model.tt)["tt(smk_ex)"]
   AHR13    <- exp(BETA13 + BETATT13*TIME)
   
   BETA14   <- coef(model.tt)["jcvi11"]
   BETATT14 <- coef(model.tt)["tt(jcvi11)"]
   AHR14    <- exp(BETA14 + BETATT14*TIME)
   





   png("Fig.ptm.AHR.png",  units="in", width=15, height=10, pointsize=20, res=300)

   
   par(mfrow=c(3,5))
   plot(TIME1, AHR1, type = "l", main="Week 24 to 32", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA1/BETATT1, lty = 2, col = "darkgray")
   plot(TIME, AHR2, type = "l", main="Deprivation Q2", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA2/BETATT2, lty = 2, col = "darkgray")
   plot(TIME, AHR3, type = "l", main="Deprivation Q3", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA3/BETATT3, lty = 2, col = "darkgray")
   plot(TIME, AHR4, type = "l", main="Deprivation Q4", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA4/BETATT4, lty = 2, col = "darkgray")
   plot(TIME, AHR5, type = "l", main="Ethnicity White", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA5/BETATT5, lty = 2, col = "darkgray")
   plot(TIME, AHR6, type = "l", main="Region London", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA6/BETATT6, lty = 2, col = "darkgray")
   plot(TIME, AHR7, type = "l", main="H/o Obesity", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA7/BETATT7, lty = 2, col = "darkgray")
   plot(TIME, AHR8, type = "l", main="H/o COVID-19", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA8/BETATT8, lty = 2, col = "darkgray")
   plot(TIME, AHR9, type = "l", main="H/o Diabetes", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA9/BETATT9, lty = 2, col = "darkgray")
   plot(TIME, AHR10, type = "l", main="H/o Pregnancy", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA10/BETATT10, lty = 2, col = "darkgray")
   plot(TIME, AHR11, type = "l", main="H/o Venous event", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA11/BETATT11, lty = 2, col = "darkgray")
   plot(TIME, AHR12, type = "l", main="H/o anticoagulant", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA12/BETATT12, lty = 2, col = "darkgray")
   plot(TIME, AHR13, type = "l", main="Ex smoker", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA13/BETATT13, lty = 2, col = "darkgray")
   plot(TIME, AHR14, type = "l", main="JCVI 11", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA14/BETATT14, lty = 2, col = "darkgray")

   mtext("AHR for non-proprotional covariates - Preterm model ", side = 3, outer = TRUE, line = -1.5, cex = 1)

   dev.off()

   print(anyNA(test.ph))
   print(any(is.nan(test.ph$x)))
   print(any(is.infinite(test.ph$time)))
   clean_const <- const[complete.cases(test.ph), ]
   
summary(test.ph[1])
   summary(test.ph[1]$time)
   plot(test.ph[41]$time,test.ph[41]$var,  main = "",
        xlab = "X", ylab = "Y", pch = 19, col = "blue")
   abline(lm(test.ph[41]$y ~ test.ph[41]$time), col = "red")
   
   newx <- seq(min(test.ph[41]$time), max(test.ph[41]$time))
   pred <- predict(lm(test.ph[41]$y ~ test.ph[41]$time), newdata = data.frame(x = newx), interval = "confidence")
   lines(newx, pred[, "lwr"], col = "red", lty = 2)
   lines(newx, pred[, "upr"], col = "red", lty = 2)
   # proptionality plot 
   
   # png("Fig.ptm.PH_part1.png",  units="in", width=14, height=12, pointsize=15, res=300)
   # par(mfrow=c(3,5))
   # plot(test.phptm[1],ylab="Week 24 to <32", main ="p.value = 0.001" )
   # plot(test.ph[42],ylab="Week 32 to 36", main ="p.value = 0.433" )
   # plot(test.ph[1],ylab="Age", main ="p.value = 0.514" )
   # plot(test.ph[2],ylab="Deprivation Q2", main ="p.value = 0.008" )
   # plot(test.ph[3],ylab="Deprivation Q3", main ="p.value = 0.039" )
   # plot(test.ph[4],ylab="Deprivation Q4", main ="p.value = 0.033" )
   # plot(test.ph[5],ylab="Deprivation Q5", main ="p.value = 0.943" )
   # plot(test.ph[6],ylab="Ethnicity Unknown", main ="p.value = 0.174" )
   # plot(test.ph[7],ylab="Ethnicity White", main ="p.value = <0.0001" )
   # plot(test.ph[8],ylab="Region East England", main ="p.value = 0.724" )
   # plot(test.ph[9],ylab="Region London", main ="p.value = 0.006" )
   # plot(test.ph[10],ylab="Region NorthEast", main ="p.value = 0.776" )
   # plot(test.ph[11],ylab="Region NorthWes", main ="p.value = 0.268" )
   # plot(test.ph[12],ylab="Region SouthEast", main ="p.value = 0.081" )
   # plot(test.ph[13],ylab="Region SouthWest", main ="p.value = 0.140" )
   # 
   # mtext("Preterm birth Schoenfeld test (global p value <0.0001) - I", side = 3, outer = TRUE, line = -1.5, cex = 1)
   # dev.off()
   # 
   # 
   # png("Fig.ptm.PH_part2.png",  units="in", width=14, height=12, pointsize=15, res=300)
   # par(mfrow=c(3,5))
   # plot(test.ph[14],ylab="Region WestMidlands", main ="p.value = 0.129" )
   # plot(test.ph[15],ylab="Region York", main ="p.value = 0.961" )
   # plot(test.ph[16],ylab="H/o Obesity", main ="p.value = 0.013" )
   # plot(test.ph[17],ylab="H/o COVID-19", main ="p.value = 0.003" )
   # plot(test.ph[18],ylab="H/o Depression anxiety", main ="p.value = 0.109" )
   # plot(test.ph[19],ylab="H/o Diabetes", main ="p.value = <0.0001" )
   # plot(test.ph[20],ylab="H/o Hypertensive disease", main ="p.value = 0.275" )
   # plot(test.ph[21],ylab="H/o PCOS", main ="p.value = 0.503" )
   # plot(test.ph[22],ylab="H/o Pregnancy", main ="p.value = 0.002" )
   # plot(test.ph[23],ylab="H/o Stillbirth", main ="p.value = 0.025" )
   # plot(test.ph[24],ylab="H/o Venous event", main ="p.value = 0.023" )
   # plot(test.ph[25],ylab="H/o Thrombophilia", main ="p.value = 0.477" )
   # plot(test.ph[26],ylab="H/o Chronic kidney disease", main ="p.value = 0.394" )
   # plot(test.ph[27],ylab="H/o lipid lowering drugs", main ="p.value = 0.287" )
   # plot(test.ph[28],ylab="H/o anticoagulant", main ="p.value = 0.036" )
   # 
   # mtext("Preterm birth Schoenfeld test (global p value <0.0001) - II", side = 3, outer = TRUE, line = -1.5, cex = 1)
   # dev.off()
   # 
   # png("Fig.ptm.PH_part3.png",  units="in", width=14, height=10, pointsize=15, res=300)
   # par(mfrow=c(2,5))
   # plot(test.ph[29],ylab="H/o antiplatelet", main ="p.value = 0.724" )
   # plot(test.ph[30],ylab="H/o COCP", main ="p.value = 0.458" )
   # plot(test.ph[31],ylab="Current smoker", main ="p.value = 0.666" )
   # plot(test.ph[32],ylab="Ex smoker", main ="p.value = 0.047" )
   # plot(test.ph[33],ylab="Never smoker", main ="p.value = 0.504" )
   # plot(test.ph[34],ylab="JCVI 10", main ="p.value = 0.286" )
   # plot(test.ph[35],ylab="JCVI 11", main ="p.value = 0.023" )
   # plot(test.ph[36],ylab="JCVI 12", main ="p.value = 0.135" )
   # plot(test.ph[37],ylab="Surgery in last year", main ="p.value = 0.617" )
   # mtext("Preterm birth Schoenfeld test (global p value <0.0001) - III", side = 3, outer = TRUE, line = -1.5, cex = 1)
   # dev.off()
   # 
   
 
   
   
   # Linearity assumption
   # Residuals vs. continuous predictor
   X3 <- survival_data$cov_age
   Y3 <- resid(model.dummy, type = "martingale")
   
   png("Fig.ptm.linearity.png",  units="in", width=10, height=10, pointsize=25, res=300)
   plot(X3, Y3, pch = 20, col = "darkgray",
        xlab = "Age", ylab = "Martingale Residual",
        main = "Residuals vs. Predictor")
   abline(h = 0)
   lines(smooth.spline(X3, Y3, df = 7), lty = 2, lwd = 2)
   dev.off()
   
   # Influential observations
   setwd("~/collab/CCU036_01/results/01_05_2024")
   
   # DFbetas <- resid(model.tt, type="dfbetas")
   DFbetas1 <- resid(model.dummy, type="dfbetas")
   head(DFbetas1)
   
   # Plot
   
   
   png("Fig.ptm.dfb_part1.png",  units="in", width=14, height=12, pointsize=15, res=300)
   par(mfrow=c(3,5))
   plot(DFbetas1[, 41], ylab="Week 24 to <32", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 42], ylab="Week 32 to36", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 1], ylab="Age", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 2], ylab="Deprivation Q2", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 3], ylab="Deprivation Q3", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 4], ylab="Deprivation Q4", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 5], ylab="Deprivation Q5", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 6], ylab="Ethnicity Unknown", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 7], ylab="Ethnicity White", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 8], ylab="Region East England", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 9], ylab="Region London", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 10], ylab="Region NorthEast", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 11], ylab="Region NorthWest", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 12], ylab="Region SouthEast", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
 
   mtext("Preterm birth influential observation - I", side = 3, outer = TRUE, line = -2, cex = 1.5)
   dev.off()
   
   
   png("Fig.ptm.dfb_part2.png",  units="in", width=14, height=12, pointsize=15, res=300)
   par(mfrow=c(3,5))
   plot(DFbetas1[, 13], ylab="Region SouthWest", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 14], ylab="Region WestMidlands", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 15], ylab="Region York", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 16], ylab="H/o Obesity", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 17], ylab="H/o COVID-19", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 18], ylab="H/o Depression anxiety", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 19], ylab="H/o Diabetes", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 20], ylab="H/o Hypertensive disease", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 21], ylab="H/o PCOS", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 22], ylab="H/o Pregnancy", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 23], ylab="H/o Stillbirth", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 24], ylab="H/o Venous event", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 25], ylab="H/o Thrombophilia", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 26], ylab="H/o Chronic kidney disease", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 27], ylab="H/o lipid lowering drugs", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
  
   mtext("Preterm birth influential observation - II", side = 3, outer = TRUE, line = -2, cex = 1.5)
   dev.off()
   
   
   png("Fig.ptm.dfb_part3.png",  units="in", width=14, height=10, pointsize=15, res=300)
   par(mfrow=c(2,5))
   plot(DFbetas1[, 28], ylab="H/o anticoagulant drugs", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 29], ylab="H/o antiplatelet", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 30], ylab="H/o COCP", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 31], ylab="Current smoker", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 2], ylab="Ex smoker", ylim=c(-0.3,0.3))
   abline(h = c(-0.32, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 33], ylab="Never smoker", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 34], ylab="JCVI 10", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 35], ylab="JCVI 11", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 36], ylab="JCVI 12", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 37], ylab="Surgery in last year", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   
   mtext("Preterm birth influential observation - III", side = 3, outer = TRUE, line = -2, cex = 1.5)
   dev.off()
   
   
   
   
   # 4. SGA MODEL NEW -------------------------------------------------------------
   
   data <- data1
   
   data$fup_start <- data$preg_start_date+168 
   data$overall_event<- data$out_bin_birth_small_gest_age 
   table(data$overall_event)
   
   #need to add each individual outcome to definition of overall_event below
   
   data$overall_pregn_event_date<-data$out_birth_small_gest_age_record_date 
   
   ################# prepare for data transform to long format ########################
   
   data$overall_pregn_event_date<-data$out_birth_small_gest_age_record_date 
   
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
   # data$fup <- ifelse(data$fup>91, 91, data$fup)# censoring at 36 weeks for preterm
   summary(data$fup)
   
   data$overall_event_time<-as.numeric(difftime(data$overall_pregn_event_date, data$fup_start, unit="days"))
   data$age_sq <- data$cov_age^2
   
   ############### stratification ###########################
   
   
   data$parity_label <-ifelse(data$cov_hx_pregnancy_max==1, "hx_of_preg", "no_hx_of_preg")
   data$parity_label <- factor(data$parity_label, levels = c("hx_of_preg", "no_hx_of_preg"))
   data$parity_label = relevel(data$parity_label, ref = "no_hx_of_preg")
   
   
   
   data <- data %>%
     mutate(COV_DEP_BIN = case_when(data$cov_deprivation<= 2 ~ 0,
                                    data$cov_deprivation >= 3 ~  1
     )
     )
   
   data$dep_label <- ifelse(data$COV_DEP_BIN==1, "high_dep", "low_dep")
   data$dep_label <- factor(data$dep_label, levels = c( "high_dep", "low_dep"))
   data$dep_label = relevel(data$dep_label, ref = "low_dep")
   
   data$dep_covid_pre <- ifelse(data$cov_hx_covid19_flag==1, "yes", "no")
   data$dep_covid_pre <- factor(data$dep_covid_pre, levels = c( "yes", "no"))
   data$dep_covid_pre = relevel(data$dep_covid_pre, ref = "no")
   
   data$dep_multi <- ifelse(data$out_bin_birth_multi_gest_max==1, "multiple", "singleton")
   data$dep_multi <- factor(data$dep_multi, levels = c( "multiple", "singleton"))
   data$dep_multi = relevel(data$dep_multi, ref = "singleton")
   
   data$eth_new <- ifelse(data$ethcat==1, "Asian", ifelse(data$ethcat==2, "Black", "Non AB"))
   data$eth_new <- factor(data$eth_new, levels = c( "Asian", "Black", "Non AB"))
   
   data$jcvi <- ifelse(data$jcvi_group_min==4, "grp_4", ifelse(data$jcvi_group_min==10, "grp_10", ifelse(data$jcvi_group_min==11, "grp_11","grp_12")))
   data$jcvi <- factor(data$jcvi, levels = c( "grp_4", "grp_10", "grp_11", "grp_12"))
   
   
   ############## seelction of the exposure and date of exposure#################
   data$vacc_covid <- data$vacc_covid_r  #"30_04_2024":(vacc_covid_r); "01_09_2023":(vacc_covid_r), '13_11_2023' :(vacc_az_r), '03_09_2023': (vacc_pf_r) ,  '04_09_2023' :(vacc2d_24wk), '05_09_2023': (vacc3d_24wk) 
   
   data <- data %>% 
     dplyr::filter(!is.na(vacc_covid))
   
   data$VACC_1ST_DUR_DATE<- as.Date(data$VACC_1ST_DUR_DATE, origin='1970-01-01')
   data$vacc_covid_date <- data$VACC_1ST_DUR_DATE
   summary(data$fup)
   
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
   
   
   td_data <-
     tmerge(
       data1 = data %>% select(all_of(vars), vacc_covid_date, overall_event_time),
       data2 = data %>% select(all_of(vars), overall_event, fup),
       id = id,
       overall_event = event(fup,  overall_event)#,
       # vacc_covid = tdc(delta_vacc_covid)
     )
   ##################  split long format dataframe into pre and post infection #####################
   
   
   
   td_data <- td_data %>% dplyr::rename(t0=tstart, t=tstop) %>% dplyr::mutate(tstart=0, tstop=t-t0)
   
   cuts_weeks_since_expo <- c(56,84)
   
   ###  data transform to long format for time from infection for those infected ####
   
   td_data<-survSplit(Surv(tstop, overall_event) ~., td_data,
                      cut=cuts_weeks_since_expo,
                      #                     event = "overall_event",
                      #                     start = "tstart",
                      #                     end = "tstop",
                      episode ="Weeks_category")
   
   
   ###########  prepare dataframe for those  with infection exposure   ##################
   td_data <- td_data %>% dplyr::mutate(tstart=tstart+t0, tstop=tstop+t0) %>% dplyr::select(-c(t0,t))
   
   td_data  <- td_data %>%
     dplyr::group_by(id) %>% dplyr::arrange(Weeks_category) %>% dplyr::mutate(last_step = ifelse(row_number()==n(),1,0))
   td_data$overall_event  <- td_data$overall_event * td_data$last_step
   
   rm(list=c( "vacc_cases"))
   data_surv <- td_data
   # write.csv(data_surv,"data_surv_sga.csv")
   survival_data <- data_surv 
   
  
   
   cov_model1 <- c("vacc_covid")
   cov_model2 <-c("vacc_covid:strata(Weeks_category)")
   cov_model3 <-c("vacc_covid*strata(Weeks_category)")
   cov_model4 <-c("vacc_covid:strata(Weeks_category) + cov_age")
   cov_model5 <-  c("vacc_covid:strata(Weeks_category) +cov_age  +  as.factor(cov_deprivation)  + cov_ethnicity_3lev + cov_hx_bmi_obesity_flag + cov_hx_covid19_flag + cov_hx_depr_anx + cov_hx_diabetes_dis + cov_hx_hypertensive_dis + cov_hx_pcos_flag + cov_hx_pregnancy_max + cov_hx_stillbirth_flag + cov_hx_venous_flag + region + cov_hx_thrombophilia_flag + cov_hx_ckd_flag + cov_meds_lipid_lowering_flag + cov_meds_anticoagulant_flag + cov_meds_antiplatelet_flag + cov_meds_cocp_flag + cov_smoking_status + cov_surg_last_yr_flag   + as.factor(jcvi_group_min) + bs(calendar_month)")
  
   f1 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model1))
   f2 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model2))
   f3 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model3))
   f4 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model4))
   f5 <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model5))
   
   model1 <- coxph(f1, survival_data, id = id)
   model2 <- coxph(f2, survival_data, id = id)
   model3 <- coxph(f3, survival_data, id = id)
   model4 <- coxph(f4, survival_data, id = id)
   model5 <- coxph(f5, survival_data, id = id)
   
   model1
   model2
   model3
   model4
   model5
   
   test.ph1 <- cox.zph(model1)
   test.ph2 <- cox.zph(model2)
   test.ph3 <- cox.zph(model3)
   test.ph4 <- cox.zph(model4)
   test.ph5 <- cox.zph(model5)
   
   test.ph1
   test.ph2
   test.ph3
   test.ph4
   test.ph5
   
   tail(model.matrix(model5))
   
   survival_data <- survival_data %>% 
     ungroup() %>% 
     mutate(deprv2  = model.matrix(model5)[, "as.factor(cov_deprivation)2"],
            deprv3  = model.matrix(model5)[, "as.factor(cov_deprivation)3"],
            deprv4  = model.matrix(model5)[, "as.factor(cov_deprivation)4"],
            deprv5 = model.matrix(model5)[, "as.factor(cov_deprivation)5"],
            eth_unkwn         = model.matrix(model5)[, "cov_ethnicity_3levUnknown"],
            eth_wht         = model.matrix(model5)[, "cov_ethnicity_3levWhite"],
            reg_east_eng         = model.matrix(model5)[, "regionEast of England"],
            reg_lond         = model.matrix(model5)[, "regionLondon"],
            reg_NE         = model.matrix(model5)[, "regionNorth East"],
            reg_NW         = model.matrix(model5)[, "regionNorth West"],
            reg_SE         = model.matrix(model5)[, "regionSouth East"],
            reg_SW         = model.matrix(model5)[, "regionSouth West"],
            reg_WM         = model.matrix(model5)[, "regionWest Midlands"],
            reg_YH         = model.matrix(model5)[, "regionYorkshire and The Humber"],
            smk_curr         = model.matrix(model5)[, "cov_smoking_statusCurrent"],
            smk_ex         = model.matrix(model5)[, "cov_smoking_statusEx"],
            smk_nev         = model.matrix(model5)[, "cov_smoking_statusNever"],
            jcvi10         = model.matrix(model5)[, "as.factor(jcvi_group_min)10"],
            jcvi11         = model.matrix(model5)[, "as.factor(jcvi_group_min)11"],
            jcvi12         = model.matrix(model5)[, "as.factor(jcvi_group_min)12"],
            cm1         = model.matrix(model5)[, "bs(calendar_month)1"],
            cm2         = model.matrix(model5)[, "bs(calendar_month)2"],
            cm3         = model.matrix(model5)[, "bs(calendar_month)3"],
            vacc1         = model.matrix(model5)[, "vacc_covid:strata(Weeks_category)Weeks_category=1"],
            vacc2         = model.matrix(model5)[, "vacc_covid:strata(Weeks_category)Weeks_category=2"],
            vacc3         = model.matrix(model5)[, "vacc_covid:strata(Weeks_category)Weeks_category=3"])
   
   cov_model.dummy <-  c("vacc1 + vacc2 + vacc3 + cov_age   + deprv2 + deprv3 + deprv4 + deprv5 + eth_unkwn + eth_wht  + reg_east_eng + reg_lond + reg_NE + reg_NW + reg_SE + reg_SW + reg_WM + reg_YH  + cov_hx_bmi_obesity_flag + cov_hx_covid19_flag + cov_hx_depr_anx + cov_hx_diabetes_dis + cov_hx_hypertensive_dis + cov_hx_pcos_flag + cov_hx_pregnancy_max + cov_hx_stillbirth_flag + cov_hx_venous_flag + cov_hx_thrombophilia_flag + cov_hx_ckd_flag + cov_meds_lipid_lowering_flag + cov_meds_anticoagulant_flag   + cov_meds_antiplatelet_flag + cov_meds_cocp_flag + smk_curr + smk_ex + smk_nev + jcvi10 + jcvi11 + jcvi12 + cov_surg_last_yr_flag   +  cm1  + cm2 + cm3")# cov_meds_anticoagulant_flag +
   f <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model.dummy))
   model.dummy <- coxph(f, survival_data, id = id)
   
   model.dummy
   
   model5
   
   test.ph <-  cox.zph(model.dummy)
   summary(test.ph)
   test.phsga <- test.ph
   
   cov_model.tt <-  c("vacc1 + vacc2+ vacc3 + cov_age + deprv2 + deprv3 + deprv4 + deprv5 + eth_unkwn + eth_wht  + reg_east_eng + reg_lond + reg_NE + reg_NW + reg_SE + reg_SW + reg_WM + reg_YH  + cov_hx_bmi_obesity_flag + cov_hx_covid19_flag + cov_hx_depr_anx + cov_hx_diabetes_dis + cov_hx_hypertensive_dis + cov_hx_pcos_flag + cov_hx_pregnancy_max + cov_hx_stillbirth_flag + cov_hx_venous_flag + cov_hx_thrombophilia_flag + cov_hx_ckd_flag + cov_meds_lipid_lowering_flag + cov_meds_anticoagulant_flag   + cov_meds_antiplatelet_flag + cov_meds_cocp_flag + smk_curr + smk_ex + smk_nev + jcvi10 + jcvi11 + jcvi12 + cov_surg_last_yr_flag   +   cm1 +  cm2 + cm3  + tt(eth_wht) + tt(reg_SE) + tt(reg_SW) + tt(reg_YH) + tt(cov_hx_bmi_obesity_flag) + tt(cov_hx_depr_anx) +tt(cov_hx_pregnancy_max) + tt(cov_hx_hypertensive_dis) +tt(smk_curr) +tt(smk_nev) +tt(cm2) +tt(cm3)")
   f <- as.formula(paste("Surv(time = tstart, time2 = tstop, event = overall_event) ~ ",  cov_model.tt))
   model.tt <- coxph(f, survival_data, id = id,tt = function(x,t,...) x*t ) ## FINAL MODEL FOR SGA
   
   model.tt
   
   setwd("~/collab/CCU036_01/results/01_05_2024")
   anv <- car::Anova(model.tt, type = 3, test = "Wald")
   
   write.csv(anv, "sga_anova.csv")
   
   class(test.ph)
   class(anv)
   library(broom)
   sga_ph <- test.ph$table
   
   write.csv(sga_ph, "sga_ph.csv")
   
   # AHR plot
   
   survival_data <- survival_data %>% 
     dplyr::mutate(fup= tstop-tstart)
   SUB    <- survival_data$overall_event == 1
   TIME   <- seq(min(survival_data$fup[SUB]),
                 max(survival_data$fup[SUB]), 1)
  
   BETA1   <- coef(model.tt)["eth_wht"]
   BETATT1 <- coef(model.tt)["tt(eth_wht)"]
   AHR1    <- exp(BETA1 + BETATT1*TIME)
   
   
   BETA2   <- coef(model.tt)["reg_SE"]
   BETATT2 <- coef(model.tt)["tt(reg_SE)"]
   AHR2    <- exp(BETA2 + BETATT2*TIME)
   
   BETA3   <- coef(model.tt)["reg_SW"]
   BETATT3 <- coef(model.tt)["tt(reg_SW)"]
   AHR3    <- exp(BETA3 + BETATT3*TIME)
   
   BETA4   <- coef(model.tt)["reg_YH"]
   BETATT4 <- coef(model.tt)["tt(reg_YH)"]
   AHR4    <- exp(BETA4 + BETATT4*TIME)
   
   BETA5   <- coef(model.tt)["cov_hx_bmi_obesity_flag"]
   BETATT5 <- coef(model.tt)["tt(cov_hx_bmi_obesity_flag)"]
   AHR5    <- exp(BETA5 + BETATT5*TIME)
   
   BETA6   <- coef(model.tt)["cov_hx_depr_anx"]
   BETATT6 <- coef(model.tt)["tt(cov_hx_depr_anx)"]
   AHR6    <- exp(BETA6 + BETATT6*TIME)
   
   BETA7   <- coef(model.tt)["cov_hx_pregnancy_max"]
   BETATT7 <- coef(model.tt)["tt(cov_hx_pregnancy_max)"]
   AHR7    <- exp(BETA7 + BETATT7*TIME)
   
   BETA8   <- coef(model.tt)["cov_hx_hypertensive_dis"]
   BETATT8 <- coef(model.tt)["tt(cov_hx_hypertensive_dis)"]
   AHR8    <- exp(BETA8 + BETATT8*TIME)
   
   BETA9   <- coef(model.tt)["smk_curr"]
   BETATT9 <- coef(model.tt)["tt(smk_curr)"]
   AHR9    <- exp(BETA9 + BETATT9*TIME)
   
   BETA10   <- coef(model.tt)["smk_nev"]
   BETATT10 <- coef(model.tt)["tt(smk_nev)"]
   AHR10    <- exp(BETA10 + BETATT10*TIME)
   
   
 
   
   
   
   
   png("Fig.sga.AHR.png",  units="in", width=15, height=10, pointsize=20, res=300)
   
   
   par(mfrow=c(2,5))
   plot(TIME, AHR1, type = "l", main="Ethnicity White", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA1/BETATT1, lty = 2, col = "darkgray")
   plot(TIME, AHR2, type = "l", main="Region SouthEast", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA2/BETATT2, lty = 2, col = "darkgray")
   plot(TIME, AHR3, type = "l", main="Region SouthWest", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA3/BETATT3, lty = 2, col = "darkgray")
   plot(TIME, AHR4, type = "l", main="Region York", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA4/BETATT4, lty = 2, col = "darkgray")
   plot(TIME, AHR5, type = "l", main="H/o Obesity", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA5/BETATT5, lty = 2, col = "darkgray")
   plot(TIME, AHR6, type = "l", main="H/o Depression anxiety", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA6/BETATT6, lty = 2, col = "darkgray")
   plot(TIME, AHR7, type = "l", main="H/o Pregnancy", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA7/BETATT7, lty = 2, col = "darkgray")
   plot(TIME, AHR8, type = "l", main="H/o Hypertensive disease", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA8/BETATT8, lty = 2, col = "darkgray")
   plot(TIME, AHR9, type = "l", main="Current smoker", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA9/BETATT9, lty = 2, col = "darkgray")
   plot(TIME, AHR10, type = "l", main="Never smoker", ylab = "AHR")
   abline(h = 1, lty = 2, col = "darkgray")
   abline(v = -1*BETA10/BETATT10, lty = 2, col = "darkgray")
   
   mtext("AHR for non-proprotional covariates - Small-for-gestational age model ", side = 3, outer = TRUE, line = -1.5, cex = 1)
   
   dev.off()
   
   
   
   # proptionality plot # NEED ATTENTION
   
   png("Fig.sga.PH_part1.png",  units="in", width=14, height=12, pointsize=15, res=300)
   par(mfrow=c(3,5))
   plot(test.ph[1],ylab="Week 24 to <32", main ="p.value = 0.894" )
   plot(test.ph[2],ylab="Week 32 to <36", main ="p.value = 0.325" )
   plot(test.ph[3],ylab="Week 36 & above", main ="p.value = 0.077" )
   plot(test.ph[4],ylab="Age", main ="p.value = 0.172" )
   plot(test.ph[5],ylab="Deprivation Q2", main ="p.value = 0.344" )
   plot(test.ph[6],ylab="Deprivation Q3", main ="p.value = 0.275" )
   plot(test.ph[7],ylab="Deprivation Q4", main ="p.value = 0.231" )
   plot(test.ph[8],ylab="Deprivation Q5", main ="p.value = 0.122" )
   plot(test.ph[9],ylab="Ethnicity Unknown", main ="p.value = 0.488" )
   plot(test.ph[10],ylab="Ethnicity White", main ="p.value = 0.005" )
   plot(test.ph[11],ylab="Region East England", main ="p.value = 0.386" )
   plot(test.ph[12],ylab="Region London", main ="p.value = 0.085" )
   plot(test.ph[13],ylab="Region NorthEast", main ="p.value = 0.582" )
   plot(test.ph[14],ylab="Region NorthWes", main ="p.value = 0.060" )
   plot(test.ph[15],ylab="Region SouthEast", main ="p.value = <0.0001" )
  

   mtext("SGA birth Schoenfeld test (global p value <0.0001) - I", side = 3, outer = TRUE, line = -1.5, cex = 1)
   dev.off()


   png("Fig.sga.PH_part2.png",  units="in", width=14, height=12, pointsize=15, res=300)
   par(mfrow=c(3,5))
   plot(test.ph[16],ylab="Region SouthWest", main ="p.value = 0.023" )
   plot(test.ph[17],ylab="Region WestMidlands", main ="p.value = 0.252" )
   plot(test.ph[18],ylab="Region York", main ="p.value = 0.001" )
   plot(test.ph[19],ylab="H/o Obesity", main ="p.value = <0.0001" )
   plot(test.ph[20],ylab="H/o COVID-19", main ="p.value = 0.531" )
   plot(test.ph[21],ylab="H/o Depression anxiety", main ="p.value = <0.0001" )
   plot(test.ph[22],ylab="H/o Diabetes", main ="p.value = 0.178" )
   plot(test.ph[23],ylab="H/o Hypertensive disease", main ="p.value = <0.0001" )
   plot(test.ph[24],ylab="H/o PCOS", main ="p.value = 0.268" )
   plot(test.ph[25],ylab="H/o Pregnancy", main ="p.value = <0.0001" )
   plot(test.ph[26],ylab="H/o Stillbirth", main ="p.value = 0.090" )
   plot(test.ph[27],ylab="H/o Venous event", main ="p.value = 0.081" )
   plot(test.ph[28],ylab="H/o Thrombophilia", main ="p.value = 0.315" )
   plot(test.ph[29],ylab="H/o Chronic kidney disease", main ="p.value = 0.854" )
   plot(test.ph[30],ylab="H/o lipid lowering drugs", main ="p.value = 0.243" )
   

   mtext("SGA birth Schoenfeld test (global p value <0.0001) - II", side = 3, outer = TRUE, line = -1.5, cex = 1)
   dev.off()

   png("Fig.sga.PH_part3.png",  units="in", width=14, height=10, pointsize=15, res=300)
   par(mfrow=c(2,5))
   plot(test.ph[31],ylab="H/o anticoagulant", main ="p.value = 0.217" )
   plot(test.ph[32],ylab="H/o antiplatelet", main ="p.value = 0.979" )
   plot(test.ph[33],ylab="H/o COCP", main ="p.value = 0.758" )
   plot(test.ph[34],ylab="Current smoker", main ="p.value = <0.0001" )
   plot(test.ph[35],ylab="Ex smoker", main ="p.value = 0.248" )
   plot(test.ph[36],ylab="Never smoker", main ="p.value = <0.0001" )
   plot(test.ph[37],ylab="JCVI 10", main ="p.value = 0.379" )
   plot(test.ph[38],ylab="JCVI 11", main ="p.value = 0.828" )
   plot(test.ph[39],ylab="JCVI 12", main ="p.value = 0.271" )
   plot(test.ph[40],ylab="Surgery in last year", main ="p.value = 0.059" )
   mtext("SGA birth Schoenfeld test (global p value <0.0001) - III", side = 3, outer = TRUE, line = -1.5, cex = 1)
   dev.off()

   
   
   
   
   # Linearity assumption
   # Residuals vs. continuous predictor
   X4 <- survival_data$cov_age
   Y4 <- resid(model.dummy, type = "martingale")
   
   png("Fig.sga.linearity.png",  units="in", width=10, height=10, pointsize=25, res=300)
   plot(X4, Y4, pch = 20, col = "darkgray",
        xlab = "Age", ylab = "Martingale Residual",
        main = "Residuals vs. Predictor")
   abline(h = 0)
   lines(smooth.spline(X4, Y4, df = 7), lty = 2, lwd = 2)
   dev.off()
   
   # Influential observations
   setwd("~/collab/CCU036_01/results/01_05_2024")
   
   # DFbetas <- resid(model.tt, type="dfbetas")
   DFbetas1 <- resid(model.dummy, type="dfbetas")
   head(DFbetas1)
   
   # Plot
   
   
   png("Fig.sga.dfb_part1.png",  units="in", width=14, height=12, pointsize=15, res=300)
   par(mfrow=c(3,5))
   plot(DFbetas1[, 1], ylab="Week 24 to <32", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 2], ylab="Week 32 to <36", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 3], ylab="Week 36 & above", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 4], ylab="Age", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 5], ylab="Deprivation Q2", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 6], ylab="Deprivation Q3", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 7], ylab="Deprivation Q4", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 8], ylab="Deprivation Q5", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 9], ylab="Ethnicity Unknown", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 10], ylab="Ethnicity White", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 11], ylab="Region East England", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 12], ylab="Region London", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 13], ylab="Region NorthEast", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 14], ylab="Region NorthWest", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 15], ylab="Region SouthEast", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   
   mtext("SGA influential observation - I", side = 3, outer = TRUE, line = -2, cex = 1.5)
   dev.off()
   
   
   png("Fig.sga.dfb_part2.png",  units="in", width=14, height=12, pointsize=15, res=300)
   par(mfrow=c(3,5))
   plot(DFbetas1[, 16], ylab="Region SouthWest", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 17], ylab="Region WestMidlands", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 18], ylab="Region York", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 19], ylab="H/o Obesity", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 20], ylab="H/o COVID-19", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 21], ylab="H/o Depression anxiety", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 22], ylab="H/o Diabetes", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 23], ylab="H/o Hypertensive disease", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 24], ylab="H/o PCOS", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 25], ylab="H/o Pregnancy", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 26], ylab="H/o Stillbirth", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 27], ylab="H/o Venous event", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 28], ylab="H/o Thrombophilia", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 29], ylab="H/o Chronic kidney disease", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 30], ylab="H/o lipid lowering drugs", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   
   mtext("SGA influential observation - II", side = 3, outer = TRUE, line = -2, cex = 1.5)
   dev.off()
   
   
   png("Fig.sga.dfb_part3.png",  units="in", width=14, height=10, pointsize=15, res=300)
   par(mfrow=c(2,5))
   plot(DFbetas1[, 31], ylab="H/o anticoagulant drugs", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 32], ylab="H/o antiplatelet", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 33], ylab="H/o COCP", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 34], ylab="Current smoker", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 35], ylab="Ex smoker", ylim=c(-0.3,0.3))
   abline(h = c(-0.32, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 36], ylab="Never smoker", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 37], ylab="JCVI 10", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 38], ylab="JCVI 11", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 39], ylab="JCVI 12", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   plot(DFbetas1[, 40], ylab="Surgery in last year", ylim=c(-0.3,0.3))
   abline(h = c(-0.2, 0.2), lty = 2, col = "red")
   
   mtext("SGA influential observation - III", side = 3, outer = TRUE, line = -2, cex = 1.5)
   dev.off()
   
   
   png("Fig.linearity.png",  units="in", width=8, height=4, pointsize=10, res=300)
   par(mfrow=c(1,4))
   plot(X3, Y3, pch = 20, col = "darkgray",
        xlab = "Age", ylab = "Martingale Residual",
        main = "Preterm birth")
   abline(h = 0)
   lines(smooth.spline(X3, Y3, df = 7), lty = 2, lwd = 2)
   plot(X4, Y4, pch = 20, col = "darkgray",
        xlab = "Age", ylab = "Martingale Residual",
        main = "SGA")
   abline(h = 0)
   lines(smooth.spline(X4, Y4, df = 7), lty = 2, lwd = 2)
   plot(X2, Y2, pch = 20, col = "darkgray",
        xlab = "Age", ylab = "Martingale Residual",
        main = "Stillbirth")
   abline(h = 0)
   lines(smooth.spline(X2, Y2, df = 7), lty = 2, lwd = 2)
   plot(X1, Y1, pch = 20, col = "darkgray",
        xlab = "Age", ylab = "Martingale Residual",
        main = "Venous event")
   abline(h = 0)
   lines(smooth.spline(X1, Y1, df = 7), lty = 2, lwd = 2)
   mtext("Linearity assumption for continous variable", side = 3, outer = TRUE, line = -1.5, cex = 1.5)
   dev.off()
   
  
   
   # single dataset for PH and anova test
   
   library(readr)
   sga_ph <- read_csv("sga_ph.csv")
   ptm_ph <- read_csv("ptm_ph.csv")
   ven_ph <- read_csv("ven_ph.csv")
   sb_ph <- read_csv("sb_ph.csv")

   
   sga_ph <- sga_ph %>% 
     dplyr::mutate(out="SGA")
  ptm_ph <- ptm_ph %>% 
    dplyr::mutate(out="PTM")
sb_ph <- sb_ph %>% 
  dplyr::mutate(out="SB")
ven_ph <- ven_ph %>% 
  dplyr::mutate(out="VEN")
ph <- rbind(sga_ph,ptm_ph, sb_ph, ven_ph)
table(ph$out)
names(ph)
ph <- ph %>% 
  dplyr::mutate(variable=`...1`) %>% 
  dplyr::select(variable, chisq, df, p, out)
write.csv(ph,"ph_test.csv")

sga_anova <- read_csv("sga_anova.csv")
sb_anova <- read_csv("sb_anova.csv")
ptm_anova <- read_csv("ptm_anova.csv")
ven_anova <- read_csv("ven_anova.csv")

sga_anova <- sga_anova %>% 
  dplyr::mutate(out="SGA")
ptm_anova <- ptm_anova %>% 
  dplyr::mutate(out="PTM")
sb_anova <- sb_anova %>% 
  dplyr::mutate(out="SB")
ven_anova <- ven_anova %>% 
  dplyr::mutate(out="VEN")
anova <- rbind(sga_anova,ptm_anova, sb_anova, ven_anova)

names(anova)
anova <- anova %>% 
dplyr::mutate(variable=`...1`) %>% 
  dplyr::select(variable, Chisq, Df, `Pr(>Chisq)`, out)
write.csv(anova,"anova.csv")
