#===============================================================================
### Estimation of the Absolute Excess Risk for the adverse pregnancy outcome###
### CCU036_01 ###

#===============================================================================
#1. Load package
#2. The event count
#3. Get the person days
#4. Get the adjusted the model estimates
#5. Outcome numbers

#1. Load package

library(stringr)
library(ggplot2)
# library(incidence)
library(tidyverse)
# library(epiR)
library(scales)
library(dplyr)
library(survival)
library(survminer)
library(gtsummary)
library(reshape2)
# library(date)
library(lubridate)
library(splines)
library(purrr)

library(purrr)
library(data.table)
library(tidyverse)
library(dplyr)
library(readr)
setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/data")


rm(list=ls())

data <- read.csv("ccu036_01_analysis_data_04092023.csv") 



################## selection criteria ########################

#data<-data %>%  rename(ID=PERSON_ID)
names(data) <- toupper(colnames(data))


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
# Updated from the ccu036_01_cox_fullperiod_15112022 script <<- replaced from EXPOSURE_VACCINE to exposure_vaccine_r
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

date <- '01_09_2023' #"01_09_2023":(vacc_covid_r), '02_09_2023' :(vacc_az_r), '03_09_2023': (vacc_pf_r) ,  '04_09_2023' :(vacc2d_24wk), '05_09_2023': (vacc3d_24wk) 

######################## run separated Cox models for all outcomes ##########################

#writeLines(c("event model adj exp estimate conf.low conf.high stad.error n.event subgroup"), paste0('/mnt/efs/arun.k.suseeladevi/CCU036_01/results/',date,'/out_dur_HRs_full_period.csv'))


cox_model_full <- function(k, y, z) {
  
  ################# prepare for data transform to long format ########################
  data$fup_start <- data$preg_start_date+84
  
  #need to add each individual outcome to definition of overall_event below
  data$overall_event<- data[[k]]
  
  table(data$overall_event)
  
  
  
  #need to add each individual outcome to definition of overall_event below
  data$overall_pregn_event_date<-data[[y]]
  
  ################# prepare for data transform to long format ########################
  
  # set dataframe for outcomes at birth
  if (k == "preterm" | k == "very_preterm" | k == "small_gest_age" | k == "stillbirth"){
    
    data <- data[which(data$preg_start_date_estimated==0),]
    data <- data[which(data$out_bin_birth_multi_gest_max==0),]
    
  }
  
  #need to add each individual outcome to definition of overall_event below
  table(data$overall_event)
  
  #need to add each individual outcome to definition of overall_event below
  data$overall_pregn_event_date<-data[[y]]
  
  
  # data <- data %>%
  #   dplyr::mutate(overall_event1 = case_when(data$overall_event == 1 & data$exp_dur_covid_1st_date< data$overall_pregn_event_date  & data$exp_dur_covid_1st_date>fup_start   ~  0,
  #                                            data$overall_event == 0  ~ 0,
  #                                            data$overall_event ==1 &  (data$exp_dur_covid_1st_date>= data$overall_pregn_event_date  | data$exp_dur_covid_1st_date<=fup_start   | is.na(data$exp_dur_covid_1st_date))~ 1
  #   )
  #   ) 
  data$overall_pregn_event_date <- as.Date(data$overall_pregn_event_date, "%d/%m/%Y")
  # data <- data %>% 
  #   dplyr::mutate(overall_pregn_event_date1=case_when(data$overall_event1 == 1  ~  data$overall_pregn_event_date
  #   )
  #   ) 
  # data$exp_dur_covid_1st_date <- as.Date(data$exp_dur_covid_1st_date, "%d/%m/%Y")
  #data$overall_pregn_event_date1 <- as.Date(data$overall_pregn_event_date1, "%d/%m/%Y")
  data$fup_start <- as.Date(data$fup_start, "%d/%m/%Y")
  data$delivery_date <- as.Date(data$delivery_date, "%d/%m/%Y")
  data <- data %>%
    dplyr::mutate(fup_end=case_when(data$overall_event == 1 ~  data$overall_pregn_event_date,
                                    # data$overall_event1 == 0  & data$exp_dur_covid_1st_date< data$overall_pregn_event_date  & data$exp_dur_covid_1st_date>fup_start   & !is.na(data$exp_dur_covid_1st_date)~ data$exp_dur_covid_1st_date,
                                    data$overall_event == 0  ~ data$delivery_date
    )
    )
  
  
  data$fup_end <- as.Date(data$fup_end, origin='1970-01-01')
  data$fup<-as.numeric(difftime(data$fup_end, data$fup_start, unit="days"))
  data$fup <- ifelse(data$fup ==0,  data$fup + 0.001, data$fup)
  # nrow(data[!is.na(data$overall_pregn_event_date1),])
  
  
  #data$overall_pregn_event_date1 <- as.Date(data$overall_pregn_event_date1, origin='1970-01-01')
  
  data$overall_event_time<-as.numeric(difftime(data$overall_pregn_event_date, data$fup_start, unit="days"))
  data$age_sq <- data$cov_age^2
  
  ############### stratification ###########################
  
  
  data$parity_label <-ifelse(data$cov_hx_pregnancy_max==1, "hx_of_preg", "no_hx_of_preg")
  data$parity_label <- factor(data$parity_label, levels = c("hx_of_preg", "no_hx_of_preg"))
  data$parity_label = relevel(data$parity_label, ref = "no_hx_of_preg")
  
  table(data$parity_label)
  
  data <- data %>%
    mutate(COV_DEP_BIN = case_when(data$cov_deprivation<= 2 ~ 0,
                                   data$cov_deprivation >= 3 ~  1
    )
    )
  
  data$dep_label <- ifelse(data$COV_DEP_BIN==1, "high_dep", "low_dep")
  data$dep_label <- factor(data$dep_label, levels = c( "high_dep", "low_dep"))
  data$dep_label = relevel(data$dep_label, ref = "low_dep")
  
  table(data$dep_label)
  
  data$dep_covid_pre <- ifelse(data$cov_hx_covid19_flag==1, "yes", "no")
  data$dep_covid_pre <- factor(data$dep_covid_pre, levels = c( "yes", "no"))
  data$dep_covid_pre = relevel(data$dep_covid_pre, ref = "no")
  
  table(data$dep_covid_pre)
  
  data$dep_multi <- ifelse(data$out_bin_birth_multi_gest_max==1, "yes", "no")
  data$dep_multi <- factor(data$dep_multi, levels = c( "yes", "no"))
  data$dep_multi = relevel(data$dep_multi, ref = "no")
  
  table(data$dep_multi)
  
  data$eth_new <- ifelse(data$ethcat==1, "Asian", ifelse(data$ethcat==2, "Black", "Non AB"))
  data$eth_new <- factor(data$eth_new, levels = c( "Asian", "Black", "Non AB"))
  
  
  table(data$eth_new)
  
  ############## seelction of the exposure and date of exposure#################
  data$vacc_covid <- data$vacc_covid_r  #"01_09_2023":(vacc_covid_r), '02_09_2023' :(vacc_az_r), '03_09_2023': (vacc_pf_r) ,  '04_09_2023' :(vacc2d_24wk), '05_09_2023': (vacc3d_24wk) 
  
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
          "cov_surg_last_yr_flag","agegp", "jcvi_group_min"
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
  
  
  write.csv(data_surv, file=paste0('D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/',date,'/data_surv',z,'.csv'))
  
}
cox_model_full("out_dur_venous", "out_dur_venous_record_date", "venous")
cox_model_full("out_bin_birth_preterm", "out_birth_preterm_record_date", "preterm")
cox_model_full("out_bin_birth_small_gest_age", "out_birth_small_gest_age_record_date", "small_gest_age")
cox_model_full("out_bin_birth_stillbirth_max", "out_birth_stillbirth_record_date", "stillbirth")




# estimating the number of women included in each analysis ----------------
library(dplyr)
date <- "05_09_2023" # "01_09_2023";"02_09_2023";"03_09_2023";"04_09_2023";"05_09_2023"
setwd(paste0("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/",date))


outcome <- c('venous','preterm','small_gest_age','stillbirth')
event_women <- data.frame()
for (i in outcome){
  d <- read_csv(paste0("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/",date,"/data_surv",i,".csv"))
  
  y <- d %>% 
    dplyr::group_by(vacc_covid) %>% 
    dplyr::summarise(ind=n_distinct(id))
  y$out <- i
  event_women <- rbind(event_women, y)
}
setwd(paste0("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/",date))
write.csv(event_women, "event_women.csv")

x <- c('01_09_2023','02_09_2023','03_09_2023','04_09_2023','05_09_2023')

event_women_all <- data.frame()
for (i in x){
  d <- read_csv(paste0("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/",i,"/event_women",".csv"))
  
  y <- d 
  y$date <- i
  event_women_all <- rbind(event_women_all, y)
}
setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/01_09_2023")
write_csv(event_women_all, "event_women_all.csv")



# exporting the HR estimates ----------------------------------------------


rm(list=ls())
date <- c('01_09_2023','02_09_2023','03_09_2023','04_09_2023','05_09_2023')
library(readr)


out <- data.frame()
for (i in date){
  y <- read_csv(paste0("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/",i,"/out_dur_HRs_full_period.csv"))
  y$date <- i
  
  out <- rbind(out, y)
}
out <- out %>% 
  dplyr::mutate(fitted=ifelse(estimate<200 & stad.error<10,1,0)) %>% 
  dplyr::filter(fitted==1)

out <- out %>% 
  dplyr::mutate(vacc=ifelse(date=='01_09_2023', "prim",
                            ifelse(date=='02_09_2023', "sens1_az",
                                   ifelse(date=='03_09_2023', "sens2_pf",
                                          ifelse(date=='04_09_2023',"sens3_2dose",
                                                 ifelse(date=='05_09_2023',"sens4_3dose",NA))))))

out <- out %>% 
  dplyr::select(- c(exp, model, fitted))

out <- out %>% 
  dplyr::mutate(subgroup=ifelse(is.na(subgroup),"main", subgroup))


out_n <- out %>% 
dplyr::mutate(drop=ifelse(subgroup!="main"& vacc!="prim",1,0))

out_n <- out_n %>% 
  dplyr::filter(drop!=1)

table(out_n$subgroup[out_n$vacc!="prim"])

# write the data for final export
# 
names(out_n)

out_n <- out_n %>% 
  dplyr::mutate(model=vacc) %>% 
  dplyr::select(-c("n.event","date","drop", "vacc"))
write_csv(out_n, "ccu036_01_hr_04092023_re.csv")
out_n$context <- "HR estimates and 95%CI from Cox model run for the risk \n of adverse pregnanacy outcomes after exposure to \n COVID vaccines. No count data is presented"




library(readr)
library(dplyr)
library(plyr)
library(scales)
library(epiR)
setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results")
event_count_prim <- read_csv("01_09_2023/event_count.csv")
event_count_sens1 <- read_csv("02_09_2023/event_count.csv")
event_count_sens2 <- read_csv("03_09_2023/event_count.csv")
event_count_sens3 <- read_csv("04_09_2023/event_count.csv")
event_count_sens4 <- read_csv("05_09_2023/event_count.csv")


event_count_prim$cat <- "Prim"
event_count_sens1$cat <- "Sens1"
event_count_sens2$cat <- "Sens2"
event_count_sens3$cat <- "Sens3"
event_count_sens4$cat <- "Sens4"


event_count_final <- rbind(event_count_prim,event_count_sens1,event_count_sens2,event_count_sens3,event_count_sens4)



event_count_final <- event_count_final %>% 
  dplyr::tibble(PYO = pdo/365.25) %>% 
  dplyr::mutate(PYO.as.String = scales::comma(PYO))


pyo <- event_count_final %>% 
  dplyr::group_by(cat, vacc_covid) %>% 
  dplyr::summarise(pyo=mean(PYO))


pyo$pyo_n <-  round_any(pyo$pyo,5)
# 
# 
pyo <- pyo %>%
  dplyr::arrange(cat, vacc_covid)
pyo %>% 
  dplyr::filter(vacc_covid==1)

event <- event_count_final$event
pdo <- event_count_final$pdo
pyo <- pdo/365.25
dat <- cbind(event, pyo)
x <- epi.conf(dat,ctype="inc.rate", method = "exact",conf.level = 0.95)
y <- x*100000

event_count_final <- event_count_final %>% 
  dplyr::mutate(pyo=pdo/365.25,
                ir=(event/pyo)*100000)


new1 <- cbind(event_count_final,y)


new1


new1$est <- round(new1$est, digits = 0)
new1$upper <- round(new1$upper, digits = 0)
new1$lower <- round(new1$lower, digits = 0)
new1

new1$event_n <- round_any(new1$event,5)

new1_0 <- new1 %>% 
  dplyr::filter(vacc_covid==0 & cat=="Prim")

new1_1 <- new1 %>% 
  dplyr::filter(vacc_covid==1 & cat=="Prim")
new1s1 <- new1 %>% 
  dplyr::filter(vacc_covid==1 & cat=="Sens1")
new1s2 <- new1 %>% 
  dplyr::filter(vacc_covid==1 & cat=="Sens2")
new1s3 <- new1 %>% 
  dplyr::filter(vacc_covid==1 & cat=="Sens3")
new1s4 <- new1 %>% 
  dplyr::filter(vacc_covid==1 & cat=="Sens4")



new_ir <- rbind(new1_0,new1_1,new1s1,new1s2,new1s3,new1s4)

new_ir <- new_ir %>% 
  dplyr::mutate(count_event=round_any(event,5)) %>% 
  dplyr::select(vacc_covid,count_event,cat,out,est,lower,upper, pyo)

# new_ir <- merge(new_ir, pyo, by.x=c("cat","vacc_covid"), by.y=c("cat","vacc_covid"))
setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/01_09_2023")
new_ir$context <- "Incidence rate (95%CI) of adverse pregnancy outcome events. Count of events rounded to the nearest multiple of five."
write_csv(new_ir,"ccu036_01_IR_04092023_all.csv")



# AER  ------------------------------------------------------------
rm(list=ls())
setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/01_09_2023")
hr_prim <- read_csv("out_dur_HRs_full_period.csv")



names(hr_prim)

table(hr_prim$adj)
table(hr_prim$subgroup)


hr <- hr_prim %>%
  dplyr::filter(adj=="full_adjusted") %>% 
  dplyr::filter(subgroup=="agegp_18_24"|subgroup=="agegp_25_29"|subgroup=="agegp_30_34"|subgroup=="agegp_35_39"|subgroup=="agegp_40_45")
table(hr$subgroup)
table(hr$event)



# event_count <- read_csv("event_count.csv")

table(event_final$vacc_covid)  
event <- event_count_final %>% 
  dplyr::filter(vacc_covid==0) %>% 
  dplyr::filter(event>10)

names(event)
names(hr)
table(hr$event)
event <- event %>% 
  dplyr::select(agegp,out,event, pdo)
hr <- hr %>% 
  dplyr::mutate(out=ifelse(event=="small_gest", "small_gest_age",event),agegp=subgroup) %>% 
  dplyr::select(out, agegp,estimate)
aer <- merge(hr, event, by.x = c("out", "agegp"), by.y = c("out", "agegp"), all.x = TRUE)
aer <- merge(aer, samplesize, by.x = c("out", "agegp"), by.y = c("out", "agegp"), all.x = TRUE)
names(aer)
# setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/data")
# data <- read.csv("ccu036_01_analysis_data_04092023.csv")
aer <- aer %>% 
  dplyr::mutate(out=toupper(out))



######AER for PRETERM########
####### age1
#####
table(aer$agegp)
input <- aer %>% 
  dplyr::filter(out=="PRETERM" & agegp=="agegp_18_24")


lifetable_1 <- data.frame(c(84:343))
colnames(lifetable_1) <- c("days")

lifetable_1$incidence_unexp <- input$event / input$pdo
lifetable_1$cumulative_survival_unexp <- cumprod(1 - lifetable_1$incidence_unexp) 
lifetable_1$hr <- input$estimate
lifetable_1$cumulative_survival_exp <- cumprod(1 - (lifetable_1$hr * lifetable_1$incidence_unexp))
lifetable_1$cumulative_difference_absolute_excess_risk <- lifetable_1$cumulative_survival_unexp - lifetable_1$cumulative_survival_exp
lifetable_1$weight <- input$wt
lifetable_1$aer_age <- input$agegp

# age2
input <- aer %>% 
  dplyr::filter(out=="PRETERM" & agegp=="agegp_25_29")


lifetable_2 <- data.frame(c(84:343))
colnames(lifetable_2) <- c("days")

lifetable_2$incidence_unexp <- input$event / input$pdo
lifetable_2$cumulative_survival_unexp <- cumprod(1 - lifetable_2$incidence_unexp) 
lifetable_2$hr <- input$estimate
lifetable_2$cumulative_survival_exp <- cumprod(1 - (lifetable_2$hr * lifetable_2$incidence_unexp))
lifetable_2$cumulative_difference_absolute_excess_risk <- lifetable_2$cumulative_survival_unexp - lifetable_2$cumulative_survival_exp
lifetable_2$weight <- input$wt
lifetable_2$aer_age <- input$agegp

#age3
input <- aer %>% 
  dplyr::filter(out=="PRETERM" & agegp=="agegp_30_34")


lifetable_3 <- data.frame(c(84:343))
colnames(lifetable_3) <- c("days")

lifetable_3$incidence_unexp <- input$event / input$pdo
lifetable_3$cumulative_survival_unexp <- cumprod(1 - lifetable_3$incidence_unexp) 
lifetable_3$hr <- input$estimate
lifetable_3$cumulative_survival_exp <- cumprod(1 - (lifetable_3$hr * lifetable_3$incidence_unexp))
lifetable_3$cumulative_difference_absolute_excess_risk <- lifetable_3$cumulative_survival_unexp - lifetable_3$cumulative_survival_exp
lifetable_3$weight <- input$wt
lifetable_3$aer_age <- input$agegp


#age4

input <- aer %>% 
  dplyr::filter(out=="PRETERM" & agegp=="agegp_35_39")


lifetable_4 <- data.frame(c(84:343))
colnames(lifetable_4) <- c("days")

lifetable_4$incidence_unexp <- input$event / input$pdo
lifetable_4$cumulative_survival_unexp <- cumprod(1 - lifetable_4$incidence_unexp) 
lifetable_4$hr <- input$estimate
lifetable_4$cumulative_survival_exp <- cumprod(1 - (lifetable_4$hr * lifetable_4$incidence_unexp))
lifetable_4$cumulative_difference_absolute_excess_risk <- lifetable_4$cumulative_survival_unexp - lifetable_4$cumulative_survival_exp
lifetable_4$weight <- input$wt
lifetable_4$aer_age <- input$agegp

#age5
input <- aer %>% 
  dplyr::filter(out=="PRETERM" & agegp=="agegp_40_45")


lifetable_5 <- data.frame(c(84:343))
colnames(lifetable_5) <- c("days")

lifetable_5$incidence_unexp <- input$event / input$pdo
lifetable_5$cumulative_survival_unexp <- cumprod(1 - lifetable_5$incidence_unexp) 
lifetable_5$hr <- input$estimate
lifetable_5$cumulative_survival_exp <- cumprod(1 - (lifetable_5$hr * lifetable_5$incidence_unexp))
lifetable_5$cumulative_difference_absolute_excess_risk <- lifetable_5$cumulative_survival_unexp - lifetable_5$cumulative_survival_exp
lifetable_5$weight <- input$wt
lifetable_5$aer_age <- input$agegp

ptm_lifetable <- rbind(lifetable_1, lifetable_2, lifetable_3, lifetable_4, lifetable_5)

ptm_lifetable <- ptm_lifetable %>% 
  dplyr::group_by(days) %>% 
  dplyr::mutate(cumulative_difference_absolute_excess_risk=weighted.mean(cumulative_difference_absolute_excess_risk,weight)) %>% 
  dplyr::ungroup() %>% 
  dplyr::select(days, cumulative_difference_absolute_excess_risk) %>% 
  unique

ptm_lifetable <- ptm_lifetable %>% 
  dplyr::filter(days==343) %>% 
  dplyr::mutate(excess_risk=cumulative_difference_absolute_excess_risk*100000)



######AER for SMALL_GEST_AGE########
####### age1
#####
table(aer$agegp)

rm("input","lifetable_1","lifetable_2","lifetable_3","lifetable_4","lifetable_5")
input <- aer %>% 
  dplyr::filter(out=="SMALL_GEST_AGE" & agegp=="agegp_18_24")


lifetable_1 <- data.frame(c(84:343))
colnames(lifetable_1) <- c("days")

lifetable_1$incidence_unexp <- input$event / input$pdo
lifetable_1$cumulative_survival_unexp <- cumprod(1 - lifetable_1$incidence_unexp) 
lifetable_1$hr <- input$estimate
lifetable_1$cumulative_survival_exp <- cumprod(1 - (lifetable_1$hr * lifetable_1$incidence_unexp))
lifetable_1$cumulative_difference_absolute_excess_risk <- lifetable_1$cumulative_survival_unexp - lifetable_1$cumulative_survival_exp
lifetable_1$weight <- input$wt
lifetable_1$aer_age <- input$agegp

# age2
input <- aer %>% 
  dplyr::filter(out=="SMALL_GEST_AGE" & agegp=="agegp_25_29")


lifetable_2 <- data.frame(c(84:343))
colnames(lifetable_2) <- c("days")

lifetable_2$incidence_unexp <- input$event / input$pdo
lifetable_2$cumulative_survival_unexp <- cumprod(1 - lifetable_2$incidence_unexp) 
lifetable_2$hr <- input$estimate
lifetable_2$cumulative_survival_exp <- cumprod(1 - (lifetable_2$hr * lifetable_2$incidence_unexp))
lifetable_2$cumulative_difference_absolute_excess_risk <- lifetable_2$cumulative_survival_unexp - lifetable_2$cumulative_survival_exp
lifetable_2$weight <- input$wt
lifetable_2$aer_age <- input$agegp

#age3
input <- aer %>% 
  dplyr::filter(out=="SMALL_GEST_AGE" & agegp=="agegp_30_34")


lifetable_3 <- data.frame(c(84:343))
colnames(lifetable_3) <- c("days")

lifetable_3$incidence_unexp <- input$event / input$pdo
lifetable_3$cumulative_survival_unexp <- cumprod(1 - lifetable_3$incidence_unexp) 
lifetable_3$hr <- input$estimate
lifetable_3$cumulative_survival_exp <- cumprod(1 - (lifetable_3$hr * lifetable_3$incidence_unexp))
lifetable_3$cumulative_difference_absolute_excess_risk <- lifetable_3$cumulative_survival_unexp - lifetable_3$cumulative_survival_exp
lifetable_3$weight <- input$wt
lifetable_3$aer_age <- input$agegp


#age4

input <- aer %>% 
  dplyr::filter(out=="SMALL_GEST_AGE" & agegp=="agegp_35_39")


lifetable_4 <- data.frame(c(84:343))
colnames(lifetable_4) <- c("days")

lifetable_4$incidence_unexp <- input$event / input$pdo
lifetable_4$cumulative_survival_unexp <- cumprod(1 - lifetable_4$incidence_unexp) 
lifetable_4$hr <- input$estimate
lifetable_4$cumulative_survival_exp <- cumprod(1 - (lifetable_4$hr * lifetable_4$incidence_unexp))
lifetable_4$cumulative_difference_absolute_excess_risk <- lifetable_4$cumulative_survival_unexp - lifetable_4$cumulative_survival_exp
lifetable_4$weight <- input$wt
lifetable_4$aer_age <- input$agegp

#age5
input <- aer %>% 
  dplyr::filter(out=="SMALL_GEST_AGE" & agegp=="agegp_40_45")


lifetable_5 <- data.frame(c(84:343))
colnames(lifetable_5) <- c("days")

lifetable_5$incidence_unexp <- input$event / input$pdo
lifetable_5$cumulative_survival_unexp <- cumprod(1 - lifetable_5$incidence_unexp) 
lifetable_5$hr <- input$estimate
lifetable_5$cumulative_survival_exp <- cumprod(1 - (lifetable_5$hr * lifetable_5$incidence_unexp))
lifetable_5$cumulative_difference_absolute_excess_risk <- lifetable_5$cumulative_survival_unexp - lifetable_5$cumulative_survival_exp
lifetable_5$weight <- input$wt
lifetable_5$aer_age <- input$agegp

sga_lifetable <- rbind(lifetable_1, lifetable_2, lifetable_3, lifetable_4, lifetable_5)

sga_lifetable <- sga_lifetable %>% 
  dplyr::group_by(days) %>% 
  dplyr::mutate(cumulative_difference_absolute_excess_risk=weighted.mean(cumulative_difference_absolute_excess_risk,weight)) %>% 
  dplyr::ungroup() %>% 
  dplyr::select(days, cumulative_difference_absolute_excess_risk) %>% 
  unique

sga_lifetable <- sga_lifetable %>% 
  dplyr::filter(days==343) %>% 
  dplyr::mutate(excess_risk=cumulative_difference_absolute_excess_risk*100000)



#####AER for STILLBIRTH########

rm("input","lifetable_1","lifetable_2","lifetable_3","lifetable_4","lifetable_5")
input <- aer %>% 
  dplyr::filter(out=="STILLBIRTH" & agegp=="agegp_18_24")


lifetable_1 <- data.frame(c(84:343))
colnames(lifetable_1) <- c("days")

lifetable_1$incidence_unexp <- input$event / input$pdo
lifetable_1$cumulative_survival_unexp <- cumprod(1 - lifetable_1$incidence_unexp) 
lifetable_1$hr <- input$estimate
lifetable_1$cumulative_survival_exp <- cumprod(1 - (lifetable_1$hr * lifetable_1$incidence_unexp))
lifetable_1$cumulative_difference_absolute_excess_risk <- lifetable_1$cumulative_survival_unexp - lifetable_1$cumulative_survival_exp
lifetable_1$weight <- input$wt
lifetable_1$aer_age <- input$agegp

# age2
input <- aer %>% 
  dplyr::filter(out=="STILLBIRTH" & agegp=="agegp_25_29")


lifetable_2 <- data.frame(c(84:343))
colnames(lifetable_2) <- c("days")

lifetable_2$incidence_unexp <- input$event / input$pdo
lifetable_2$cumulative_survival_unexp <- cumprod(1 - lifetable_2$incidence_unexp) 
lifetable_2$hr <- input$estimate
lifetable_2$cumulative_survival_exp <- cumprod(1 - (lifetable_2$hr * lifetable_2$incidence_unexp))
lifetable_2$cumulative_difference_absolute_excess_risk <- lifetable_2$cumulative_survival_unexp - lifetable_2$cumulative_survival_exp
lifetable_2$weight <- input$wt
lifetable_2$aer_age <- input$agegp

#age3
input <- aer %>% 
  dplyr::filter(out=="STILLBIRTH" & agegp=="agegp_30_34")


lifetable_3 <- data.frame(c(84:343))
colnames(lifetable_3) <- c("days")

lifetable_3$incidence_unexp <- input$event / input$pdo
lifetable_3$cumulative_survival_unexp <- cumprod(1 - lifetable_3$incidence_unexp) 
lifetable_3$hr <- input$estimate
lifetable_3$cumulative_survival_exp <- cumprod(1 - (lifetable_3$hr * lifetable_3$incidence_unexp))
lifetable_3$cumulative_difference_absolute_excess_risk <- lifetable_3$cumulative_survival_unexp - lifetable_3$cumulative_survival_exp
lifetable_3$weight <- input$wt
lifetable_3$aer_age <- input$agegp


#age4

input <- aer %>% 
  dplyr::filter(out=="STILLBIRTH" & agegp=="agegp_35_39")


lifetable_4 <- data.frame(c(84:343))
colnames(lifetable_4) <- c("days")

lifetable_4$incidence_unexp <- input$event / input$pdo
lifetable_4$cumulative_survival_unexp <- cumprod(1 - lifetable_4$incidence_unexp) 
lifetable_4$hr <- input$estimate
lifetable_4$cumulative_survival_exp <- cumprod(1 - (lifetable_4$hr * lifetable_4$incidence_unexp))
lifetable_4$cumulative_difference_absolute_excess_risk <- lifetable_4$cumulative_survival_unexp - lifetable_4$cumulative_survival_exp
lifetable_4$weight <- input$wt
lifetable_4$aer_age <- input$agegp

#age5
input <- aer %>% 
  dplyr::filter(out=="STILLBIRTH" & agegp=="agegp_40_45")


lifetable_5 <- data.frame(c(84:343))
colnames(lifetable_5) <- c("days")

lifetable_5$incidence_unexp <- input$event / input$pdo
lifetable_5$cumulative_survival_unexp <- cumprod(1 - lifetable_5$incidence_unexp) 
lifetable_5$hr <- input$estimate
lifetable_5$cumulative_survival_exp <- cumprod(1 - (lifetable_5$hr * lifetable_5$incidence_unexp))
lifetable_5$cumulative_difference_absolute_excess_risk <- lifetable_5$cumulative_survival_unexp - lifetable_5$cumulative_survival_exp
lifetable_5$weight <- input$wt
lifetable_5$aer_age <- input$agegp

sb_lifetable <- rbind(lifetable_1, lifetable_2, lifetable_3, lifetable_4, lifetable_5)

sb_lifetable <- sb_lifetable %>% 
  dplyr::group_by(days) %>% 
  dplyr::mutate(cumulative_difference_absolute_excess_risk=weighted.mean(cumulative_difference_absolute_excess_risk,weight)) %>% 
  dplyr::ungroup() %>% 
  dplyr::select(days, cumulative_difference_absolute_excess_risk) %>% 
  unique

sb_lifetable <- sb_lifetable %>% 
  dplyr::filter(days==343) %>% 
  dplyr::mutate(excess_risk=cumulative_difference_absolute_excess_risk*100000)
#####AER for VENOUS########

rm("input","lifetable_1","lifetable_2","lifetable_3","lifetable_4","lifetable_5")
input <- aer %>% 
  dplyr::filter(out=="VENOUS" & agegp=="agegp_18_24")


lifetable_1 <- data.frame(c(84:343))
colnames(lifetable_1) <- c("days")

lifetable_1$incidence_unexp <- input$event / input$pdo
lifetable_1$cumulative_survival_unexp <- cumprod(1 - lifetable_1$incidence_unexp) 
lifetable_1$hr <- input$estimate
lifetable_1$cumulative_survival_exp <- cumprod(1 - (lifetable_1$hr * lifetable_1$incidence_unexp))
lifetable_1$cumulative_difference_absolute_excess_risk <- lifetable_1$cumulative_survival_unexp - lifetable_1$cumulative_survival_exp
lifetable_1$weight <- input$wt
lifetable_1$aer_age <- input$agegp

# age2
input <- aer %>% 
  dplyr::filter(out=="VENOUS" & agegp=="agegp_25_29")


lifetable_2 <- data.frame(c(84:343))
colnames(lifetable_2) <- c("days")

lifetable_2$incidence_unexp <- input$event / input$pdo
lifetable_2$cumulative_survival_unexp <- cumprod(1 - lifetable_2$incidence_unexp) 
lifetable_2$hr <- input$estimate
lifetable_2$cumulative_survival_exp <- cumprod(1 - (lifetable_2$hr * lifetable_2$incidence_unexp))
lifetable_2$cumulative_difference_absolute_excess_risk <- lifetable_2$cumulative_survival_unexp - lifetable_2$cumulative_survival_exp
lifetable_2$weight <- input$wt
lifetable_2$aer_age <- input$agegp

#age3
input <- aer %>% 
  dplyr::filter(out=="VENOUS" & agegp=="agegp_30_34")


lifetable_3 <- data.frame(c(84:343))
colnames(lifetable_3) <- c("days")

lifetable_3$incidence_unexp <- input$event / input$pdo
lifetable_3$cumulative_survival_unexp <- cumprod(1 - lifetable_3$incidence_unexp) 
lifetable_3$hr <- input$estimate
lifetable_3$cumulative_survival_exp <- cumprod(1 - (lifetable_3$hr * lifetable_3$incidence_unexp))
lifetable_3$cumulative_difference_absolute_excess_risk <- lifetable_3$cumulative_survival_unexp - lifetable_3$cumulative_survival_exp
lifetable_3$weight <- input$wt
lifetable_3$aer_age <- input$agegp


#age4

input <- aer %>% 
  dplyr::filter(out=="VENOUS" & agegp=="agegp_35_39")


lifetable_4 <- data.frame(c(84:343))
colnames(lifetable_4) <- c("days")

lifetable_4$incidence_unexp <- input$event / input$pdo
lifetable_4$cumulative_survival_unexp <- cumprod(1 - lifetable_4$incidence_unexp) 
lifetable_4$hr <- input$estimate
lifetable_4$cumulative_survival_exp <- cumprod(1 - (lifetable_4$hr * lifetable_4$incidence_unexp))
lifetable_4$cumulative_difference_absolute_excess_risk <- lifetable_4$cumulative_survival_unexp - lifetable_4$cumulative_survival_exp
lifetable_4$weight <- input$wt
lifetable_4$aer_age <- input$agegp

ven_lifetable <- rbind(lifetable_1, lifetable_2, lifetable_3, lifetable_4)

ven_lifetable <- ven_lifetable %>% 
  dplyr::group_by(days) %>% 
  dplyr::mutate(cumulative_difference_absolute_excess_risk=weighted.mean(cumulative_difference_absolute_excess_risk,weight)) %>% 
  dplyr::ungroup() %>% 
  dplyr::select(days, cumulative_difference_absolute_excess_risk) %>% 
  unique

ven_lifetable <- ven_lifetable %>% 
  dplyr::filter(days==343) %>% 
  dplyr::mutate(excess_risk=cumulative_difference_absolute_excess_risk*100000)

AER_all_outcome <- rbind(ptm_lifetable, sga_lifetable,sb_lifetable, ven_lifetable)

setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/01_09_2023")


write_csv(AER_all_outcome,"AER_alloutcome_19012024.csv")




#####AZ vaccine AER for VENOUS########
#####
#####
#####



setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/02_09_2023")

library(readr)
# sb <- read_csv("data_survstillbirth.csv")
# sga <- read_csv("data_survsmall_gest_age.csv")
# ptm <- read_csv("data_survpreterm.csv")
ven <- read_csv("data_survvenous.csv")



ven_ss <- ven %>% 
  dplyr::group_by(agegp) %>% 
  dplyr::summarise(samplesize=n()) %>% 
  dplyr::mutate(out="venous")


samplesize <- ven_ss

samplesize <-  samplesize %>% 
  dplyr::mutate(cut=ifelse(out=="venous"& agegp=="agegp_40_45",1,0)) %>% 
  dplyr::group_by(out) %>% 
  dplyr::mutate(sum=sum(samplesize),wt=samplesize/sum(samplesize)) %>% 
  dplyr::filter(cut!=1)
# Event count and Person time post 19 jan 2024---------------------------------------------
date <- '02_09_2023' #"01_09_2023":(vacc_covid_r), '02_09_2023' :(vacc_az_r), '03_09_2023': (vacc_pf_r) ,  '04_09_2023' :(vacc2d_24wk), '05_09_2023': (vacc3d_24wk)


outcome <- c('venous','preterm','small_gest_age','stillbirth')
event_final <- data.frame()
for (i in outcome){
  d <- read_csv(paste0("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/",date,"/data_surv",i,".csv"))
  # d <- read_csv(paste0("/mnt/efs/arun.k.suseeladevi/CCU036_01/results/07_01_2023/data_survGEST_DIABETES.csv"))
  y <- d %>% 
    dplyr::mutate(pdays=(tstop-tstart)+1) %>% 
    dplyr::group_by(agegp,vacc_covid) %>% 
    dplyr::summarise( event=sum(overall_event),
                      pdo=sum(pdays))
  y$out <- i
  event_final <- rbind(event_final, y)
}
setwd(paste0("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/",date))
write.csv(event_final, "event_count_final.csv")
# AER 19012024 ------------------------------------------------------------

setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/02_09_2023")
hr_sen1 <- read_csv("out_dur_HRs_full_period.csv")




hr <- hr_sen1 %>%
  dplyr::filter(adj=="full_adjusted") %>% 
  dplyr::filter(event=="venous") %>% 
  dplyr::filter(subgroup=="agegp_18_24"|subgroup=="agegp_25_29"|subgroup=="agegp_30_34"|subgroup=="agegp_35_39"|subgroup=="agegp_40_45")
table(hr$subgroup)
table(hr$event)



# event_count <- read_csv("event_count.csv")

table(event_final$vacc_covid)  
event <- event_final %>% 
  dplyr::filter(out=="venous") %>% 
  dplyr::filter(vacc_covid==0) %>% 
  dplyr::filter(event>10)

names(event)
names(hr)
table(hr$event)
event <- event %>% 
  dplyr::select(agegp,out,event, pdo)
hr <- hr %>% 
  dplyr::mutate(out=ifelse(event=="small_gest", "small_gest_age",event),agegp=subgroup) %>% 
  dplyr::select(out, agegp,estimate)
aer <- merge(hr, event, by.x = c("out", "agegp"), by.y = c("out", "agegp"), all.x = TRUE)
aer <- merge(aer, samplesize, by.x = c("out", "agegp"), by.y = c("out", "agegp"), all.x = TRUE)
aer <- aer %>% 
  dplyr::mutate(out=toupper(out))

#age1
rm("input","lifetable_1","lifetable_2","lifetable_3","lifetable_4","lifetable_5")
input <- aer %>% 
  dplyr::filter(out=="VENOUS" & agegp=="agegp_18_24")


lifetable_1 <- data.frame(c(84:343))
colnames(lifetable_1) <- c("days")

lifetable_1$incidence_unexp <- input$event / input$pdo
lifetable_1$cumulative_survival_unexp <- cumprod(1 - lifetable_1$incidence_unexp) 
lifetable_1$hr <- input$estimate
lifetable_1$cumulative_survival_exp <- cumprod(1 - (lifetable_1$hr * lifetable_1$incidence_unexp))
lifetable_1$cumulative_difference_absolute_excess_risk <- lifetable_1$cumulative_survival_unexp - lifetable_1$cumulative_survival_exp
lifetable_1$weight <- input$wt
lifetable_1$aer_age <- input$agegp

# age2
input <- aer %>% 
  dplyr::filter(out=="VENOUS" & agegp=="agegp_25_29")


lifetable_2 <- data.frame(c(84:343))
colnames(lifetable_2) <- c("days")

lifetable_2$incidence_unexp <- input$event / input$pdo
lifetable_2$cumulative_survival_unexp <- cumprod(1 - lifetable_2$incidence_unexp) 
lifetable_2$hr <- input$estimate
lifetable_2$cumulative_survival_exp <- cumprod(1 - (lifetable_2$hr * lifetable_2$incidence_unexp))
lifetable_2$cumulative_difference_absolute_excess_risk <- lifetable_2$cumulative_survival_unexp - lifetable_2$cumulative_survival_exp
lifetable_2$weight <- input$wt
lifetable_2$aer_age <- input$agegp

#age3
input <- aer %>% 
  dplyr::filter(out=="VENOUS" & agegp=="agegp_30_34")


lifetable_3 <- data.frame(c(84:343))
colnames(lifetable_3) <- c("days")

lifetable_3$incidence_unexp <- input$event / input$pdo
lifetable_3$cumulative_survival_unexp <- cumprod(1 - lifetable_3$incidence_unexp) 
lifetable_3$hr <- input$estimate
lifetable_3$cumulative_survival_exp <- cumprod(1 - (lifetable_3$hr * lifetable_3$incidence_unexp))
lifetable_3$cumulative_difference_absolute_excess_risk <- lifetable_3$cumulative_survival_unexp - lifetable_3$cumulative_survival_exp
lifetable_3$weight <- input$wt
lifetable_3$aer_age <- input$agegp


#age4

input <- aer %>% 
  dplyr::filter(out=="VENOUS" & agegp=="agegp_35_39")


lifetable_4 <- data.frame(c(84:343))
colnames(lifetable_4) <- c("days")

lifetable_4$incidence_unexp <- input$event / input$pdo
lifetable_4$cumulative_survival_unexp <- cumprod(1 - lifetable_4$incidence_unexp) 
lifetable_4$hr <- input$estimate
lifetable_4$cumulative_survival_exp <- cumprod(1 - (lifetable_4$hr * lifetable_4$incidence_unexp))
lifetable_4$cumulative_difference_absolute_excess_risk <- lifetable_4$cumulative_survival_unexp - lifetable_4$cumulative_survival_exp
lifetable_4$weight <- input$wt
lifetable_4$aer_age <- input$agegp

ven_lifetable <- rbind(lifetable_1, lifetable_2, lifetable_3, lifetable_4)

ven_lifetable <- ven_lifetable %>% 
  dplyr::group_by(days) %>% 
  dplyr::mutate(cumulative_difference_absolute_excess_risk=weighted.mean(cumulative_difference_absolute_excess_risk,weight)) %>% 
  dplyr::ungroup() %>% 
  dplyr::select(days, cumulative_difference_absolute_excess_risk) %>% 
  unique

ven_lifetable <- ven_lifetable %>% 
  dplyr::filter(days==343) %>% 
  dplyr::mutate(excess_risk=cumulative_difference_absolute_excess_risk*100000)


setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/02_09_2023")


write_csv(ven_lifetable,"AER_AZ_ven_19012024.csv")




#####PF vaccine AER for VENOUS########
#####
#####
#####



setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/03_09_2023")

library(readr)
# sb <- read_csv("data_survstillbirth.csv")
# sga <- read_csv("data_survsmall_gest_age.csv")
# ptm <- read_csv("data_survpreterm.csv")
ven <- read_csv("data_survvenous.csv")



ven_ss <- ven %>% 
  dplyr::group_by(agegp) %>% 
  dplyr::summarise(samplesize=n()) %>% 
  dplyr::mutate(out="venous")


samplesize <- ven_ss

samplesize <-  samplesize %>% 
  dplyr::mutate(cut=ifelse(out=="venous"& agegp=="agegp_40_45",1,0)) %>% 
  dplyr::group_by(out) %>% 
  dplyr::mutate(sum=sum(samplesize),wt=samplesize/sum(samplesize)) %>% 
  dplyr::filter(cut!=1)
# Event count and Person time post 19 jan 2024---------------------------------------------
date <- '03_09_2023' #"01_09_2023":(vacc_covid_r), '02_09_2023' :(vacc_az_r), '03_09_2023': (vacc_pf_r) ,  '04_09_2023' :(vacc2d_24wk), '05_09_2023': (vacc3d_24wk)


outcome <- c('venous')
event_final <- data.frame()
for (i in outcome){
  d <- read_csv(paste0("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/",date,"/data_surv",i,".csv"))
  # d <- read_csv(paste0("/mnt/efs/arun.k.suseeladevi/CCU036_01/results/07_01_2023/data_survGEST_DIABETES.csv"))
  y <- d %>% 
    dplyr::mutate(pdays=(tstop-tstart)+1) %>% 
    dplyr::group_by(agegp,vacc_covid) %>% 
    dplyr::summarise( event=sum(overall_event),
                      pdo=sum(pdays))
  y$out <- i
  event_final <- rbind(event_final, y)
}
setwd(paste0("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/",date))
write.csv(event_final, "event_count_final.csv")
# AER 19012024 ------------------------------------------------------------

setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/03_09_2023")
hr_sen2 <- read_csv("out_dur_HRs_full_period.csv")




hr <- hr_sen2 %>%
  dplyr::filter(adj=="full_adjusted") %>% 
  dplyr::filter(event=="venous") %>% 
  dplyr::filter(subgroup=="agegp_18_24"|subgroup=="agegp_25_29"|subgroup=="agegp_30_34"|subgroup=="agegp_35_39"|subgroup=="agegp_40_45")
table(hr$subgroup)
table(hr$event)




event <- event_final %>% 
  dplyr::filter(out=="venous") %>% 
  dplyr::filter(vacc_covid==0) %>% 
  dplyr::filter(event>10)

names(event)
names(hr)
table(hr$event)
event <- event %>% 
  dplyr::select(agegp,out,event, pdo)
hr <- hr %>% 
  dplyr::mutate(out=ifelse(event=="small_gest", "small_gest_age",event),agegp=subgroup) %>% 
  dplyr::select(out, agegp,estimate)
aer <- merge(hr, event, by.x = c("out", "agegp"), by.y = c("out", "agegp"), all.x = TRUE)
aer <- merge(aer, samplesize, by.x = c("out", "agegp"), by.y = c("out", "agegp"), all.x = TRUE)
aer <- aer %>% 
  dplyr::mutate(out=toupper(out))

#age1
rm("input","lifetable_1","lifetable_2","lifetable_3","lifetable_4","lifetable_5")
input <- aer %>% 
  dplyr::filter(out=="VENOUS" & agegp=="agegp_18_24")


lifetable_1 <- data.frame(c(84:343))
colnames(lifetable_1) <- c("days")

lifetable_1$incidence_unexp <- input$event / input$pdo
lifetable_1$cumulative_survival_unexp <- cumprod(1 - lifetable_1$incidence_unexp) 
lifetable_1$hr <- input$estimate
lifetable_1$cumulative_survival_exp <- cumprod(1 - (lifetable_1$hr * lifetable_1$incidence_unexp))
lifetable_1$cumulative_difference_absolute_excess_risk <- lifetable_1$cumulative_survival_unexp - lifetable_1$cumulative_survival_exp
lifetable_1$weight <- input$wt
lifetable_1$aer_age <- input$agegp

# age2
input <- aer %>% 
  dplyr::filter(out=="VENOUS" & agegp=="agegp_25_29")


lifetable_2 <- data.frame(c(84:343))
colnames(lifetable_2) <- c("days")

lifetable_2$incidence_unexp <- input$event / input$pdo
lifetable_2$cumulative_survival_unexp <- cumprod(1 - lifetable_2$incidence_unexp) 
lifetable_2$hr <- input$estimate
lifetable_2$cumulative_survival_exp <- cumprod(1 - (lifetable_2$hr * lifetable_2$incidence_unexp))
lifetable_2$cumulative_difference_absolute_excess_risk <- lifetable_2$cumulative_survival_unexp - lifetable_2$cumulative_survival_exp
lifetable_2$weight <- input$wt
lifetable_2$aer_age <- input$agegp

#age3
input <- aer %>% 
  dplyr::filter(out=="VENOUS" & agegp=="agegp_30_34")


lifetable_3 <- data.frame(c(84:343))
colnames(lifetable_3) <- c("days")

lifetable_3$incidence_unexp <- input$event / input$pdo
lifetable_3$cumulative_survival_unexp <- cumprod(1 - lifetable_3$incidence_unexp) 
lifetable_3$hr <- input$estimate
lifetable_3$cumulative_survival_exp <- cumprod(1 - (lifetable_3$hr * lifetable_3$incidence_unexp))
lifetable_3$cumulative_difference_absolute_excess_risk <- lifetable_3$cumulative_survival_unexp - lifetable_3$cumulative_survival_exp
lifetable_3$weight <- input$wt
lifetable_3$aer_age <- input$agegp


#age4

input <- aer %>% 
  dplyr::filter(out=="VENOUS" & agegp=="agegp_35_39")


lifetable_4 <- data.frame(c(84:343))
colnames(lifetable_4) <- c("days")

lifetable_4$incidence_unexp <- input$event / input$pdo
lifetable_4$cumulative_survival_unexp <- cumprod(1 - lifetable_4$incidence_unexp) 
lifetable_4$hr <- input$estimate
lifetable_4$cumulative_survival_exp <- cumprod(1 - (lifetable_4$hr * lifetable_4$incidence_unexp))
lifetable_4$cumulative_difference_absolute_excess_risk <- lifetable_4$cumulative_survival_unexp - lifetable_4$cumulative_survival_exp
lifetable_4$weight <- input$wt
lifetable_4$aer_age <- input$agegp

ven_lifetable <- rbind(lifetable_1, lifetable_2, lifetable_3, lifetable_4)

ven_lifetable <- ven_lifetable %>% 
  dplyr::group_by(days) %>% 
  dplyr::mutate(cumulative_difference_absolute_excess_risk=weighted.mean(cumulative_difference_absolute_excess_risk,weight)) %>% 
  dplyr::ungroup() %>% 
  dplyr::select(days, cumulative_difference_absolute_excess_risk) %>% 
  unique

ven_lifetable <- ven_lifetable %>% 
  dplyr::filter(days==343) %>% 
  dplyr::mutate(excess_risk=cumulative_difference_absolute_excess_risk*100000)


setwd("D:/PhotonUser/My Files/Home Folder/collab/CCU036_01/results/03_09_2023")


write_csv(ven_lifetable,"AER_PF_ven_19012024.csv")




