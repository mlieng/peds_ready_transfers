# The goal of this file is to extract 2012 data and merge with other datasets 
# to get ready for analysis


#
# Set-up ----
#

# Import custom functions

# Import libraries
library(RPostgres) #database
library(tidyverse) # includes 
library(feather)
# dplyr(data munging), ggplot2 (plotting), tidyr (tidy data)
# library(dplyr) # data munging

# Import files

oshpd_folder = file.path(getwd(),'local_data','oshpd_full_data')

linked_pilot = read.csv('local_data/2018.05.22_linked_pilot.csv', 
                        header=TRUE, row.names=1)
utilization = file.path('data_dropbox','oshpd_utilization',
                        '2012_final_data_set_Hosp12_util_data_FINAL.csv')
utilization = read.csv(utilization, header=TRUE)
urban_classif = file.path(getwd(),'data_dropbox','cdc',
                          'NCHSURCodes2013.xlsx')
urban_classif = read.xls(urban_classif, header=TRUE)
# county # -> county name
county_map = file.path(getwd(),'data_symlink', '2019.02.14_county.csv')
county_map = read.csv(county_map, header=TRUE)
# icd 9 cm -> 18 groups
ccs_multi_dx_tool = file.path('data_symlink', 'hcup_ahrq_ccs', 'Multi_Level_CCS_2015', 'ccs_multi_dx_tool_2015.csv')
ccs_multi_dx_tool = read.csv(ccs_multi_dx_tool, header=TRUE)

hospitals = read.csv('local_data/2018.05.21_hospital_id_map.csv', 
                     header=TRUE, row.names=1)


#
# Connect to database----
#

con = dbConnect(RPostgres::Postgres(),
                dbname='oshpd_postgres', 
                host="localhost", port=5433,
                user='postgres',
                password='')
dbListTables(con)


#
# Pull from database----
#

combined_peds = tbl(con, 'combined_peds') # make a dplyr table object
start.time = Sys.time()
# pull from SQL database
encounters = combined_peds %>% 
  # filter, only choose individuals less than 18
  filter(start_year==2012 & age<18) %>% 
  select(id, database, rln, birthdate, age, age_days, pat_zip, pat_county, sex,
         race_group, lang_spoken, start_date, end_date, 
         start_month, start_quarter, start_year, end_year, oshpd_id, 
         ed_disp, ip_source_site, ip_source_route, ip_source_license, ip_disp,
         payer, insurance, ccs_dx_prin, dx_prin, fac_county) %>% 
  rename(sql_id = id) %>%
  collect()
end.time=Sys.time()
print(paste('Time Finished: ', end.time))
print(end.time-start.time)


#
# Post-processing and categorization ----
#

data_  = encounters 
# filter inpatient so that they came through the ed
data_ = data_ %>% filter(database=='edd' | ip_source_route==1)
# replace 'missing'
data_$rln[data_$rln=="---------"] = NA
# clean up neonate
data_$age_days[data_$age>=1]=NA #replace 0 with NULL 
data_$neonate = data_$age_days<28 %>% replace_na(FALSE)
data_$neonate = data_$neonate %>% replace_na(FALSE) # replace age>1 with FALSE
data_ = data_ %>%
  mutate(
    start_date = ymd(start_date),
    end_date = ymd(end_date),
    ip_course = paste0(ip_source_site, ip_source_license, 
                       ip_source_route),
    has.rln = !is.na(rln),
    
    # transferred = ed_disp %in% c(2,5),
    # admitted = ip_source_route==1,
    # dc_home = ed_disp==1,
    
    outcome = case_when(
      ed_disp %in% c(2,5) ~ 'transferred',
      ip_source_route==1 ~'admitted',
      ed_disp==1~'dc home'
    ),
    
    transferred = outcome=='transferred',
    transferred.char = as.character(transferred),
    # copied from 2018-11-12_Transfer RFS
    # recode language spoken
    lang_spoken2 = recode(
      lang_spoken, ENG='English', SPA='Spanish',
      CHI='Chinese',VIE='Vietnamese',ARA='Arabic',`999`='Unknown/I/B', 
      `*` = 'Unknown/I/B',`-`='Unknown/I/B',.default='Other') %>% 
      # specify order of factor
      factor(levels = c('English', 'Spanish','Arabic', 'Chinese', 
                        'Vietnamese', 'Other', 'Unknown/I/B')),
    
    # recode race group
    race_group2 = recode(
      race_group, `1`='White', `2`='Black', `3`='Hispanic', `4`='A/PI', 
      `5`='AI/AN', `6`='Other', `0`='Unknown') %>% 
      # specify order of factor
      factor(
        levels = c('Hispanic', 'White','Black','A/PI',
                   'Other','Unknown','AI/AN')),
    
    race_group3 = recode(
      race_group2,
      `White` = 'White',
      `Black` = 'Black',
      `Hispanic` = 'Hispanic/Latino',
      `A/PI` = 'Asian/Pacific Islander',
      .default = 'Other'
    ) %>% factor(levels=c('White', 'Hispanic/Latino', 'Black', 'Asian/Pacific Islander',
                          'Other')),#relevel('White')
    
    # copied from 2018-11-16 test descriptive
    sex2 = recode(
      sex, F='Female', M='Male') %>% factor(levels=c('Male', 'Female')),
    
    # age categories from NICHD "Standard 6: Age Groups for Pediatric Trials"
    age_cats = case_when(
      age_days<28 ~ "0-27d",
      age_days>=28 & age<1 ~ "28d-12m",
      age>=1 & age<2 ~"13-23m",
      age>=2 & age<6 ~"2-5y",
      age>=6 & age<12~"6-11y",
      age>=12 & age<18~"12-17y",
      age>=18 & age<21 ~"18-21y" #& age<22)
    ) %>% factor(
      levels=c('0-27d', '28d-12m', '13-23m', 
               '2-5y', '6-11y', '12-17y', '18-21y')),
    
    age_cats2 = case_when(
      age_days<28 ~ "0-27d",
      age_days>=28 & age<1 ~ "28d-12m",
      age>=1 & age<18 ~"1y-17y",
      age>=18 & age<21 ~NA_character_) %>% factor() %>% relevel('1y-17y'),
    
    start_month.factor = factor(start_month, levels=c(1:12) %>% as.character()),
    enc_season = case_when(
      start_month %in% c(12,1,2) ~ "Winter",
      start_month %in% c(3,4,5) ~ "Spring",
      start_month %in% c(6,7,8) ~ "Summer",
      start_month %in% c(9,10,11) ~ "Fall") %>% 
      factor(levels = c('Winter', 'Spring', 'Summer', 'Fall')),
    
    # ccs mental health #TODO double check if this is correct
    #  ccs_mental_health = ccs_dx_prin >= 650 & ccs_dx_prin <= 670
    
    insurance2 = recode(insurance, 
                        `Private`='Private',
                        `Public-Medicare-MediCal`='Public',
                        `Self-Pay`='Uninsured/self-pay', #self-pay is the same as uninsured
                        .default='Other'
    ) %>% factor(levels=c('Private', 'Public', 'Uninsured/self-pay',
                          'Other'))
  )
# exclude rare events and neonates
data_ = data_ %>% filter(!is.na(outcome)) # %>% filter(!neonate)
# data_ %>% select(age, age_days, age_cats, neonate, neonate_filled) %>% head(15)
# data_ %>% select(age, age_days, age_cats, neonate, neonate_filled) %>% filter(age<3)%>% head(15)


#
# __Clinical Classifications Software (18 categories) ----
#

ccs = ccs_multi_dx_tool %>% 
  # clean up
  mutate(dx_prin = X.ICD.9.CM.CODE. %>% str_remove_all("\'") %>% trimws(), 
         ccs_group = as.integer(X.CCS.LVL.1.),
         # combine 'Mental illness' with '
         ccs_label = X.CCS.LVL.1.LABEL. %>% str_replace('Mental illness', 'Mental Illness')) %>%
  select(dx_prin, ccs_group, ccs_label)

# join with data
data_ = left_join(data_, ccs, by='dx_prin')
data_ = data_ %>% mutate(
  ccs_mental_health = ccs_label =='Mental Illness',
  ccs_injury = ccs_label == 'Injury and poisoning'
)

#
# __NCHS Urban Rural Designation----
#

NCHS_urban_rural_scheme = c(
  'Large central metro', 'Large fringe metro', 'Medium metro',
  'Small metro','Micropolitan','Noncore')
urban_classif_reduced = urban_classif %>% filter(State.Abr.=='CA') %>%
  mutate(county = County.name %>% str_replace_all(" County", ""),
         fac_county.name = county,
         pat_county.name = county,
         urban_rural.int = X2013.code,
         urban_rural.factor = factor(X2013.code, levels=c(1,2,3,4,5,6),
                                     labels=NCHS_urban_rural_scheme),
         fac_urban_rural = urban_rural.factor,
         pat_urban_rural = urban_rural.factor,
         urban_rural.char = recode(X2013.code,
                                   `1` = 'Large central metro', `2` = 'Large fringe metro',
                                   `3` = 'Medium metro', `4` = 'Small metro',
                                   `5` = 'Micropolitan', `6` =  'Noncore')
  ) 
# recode OSHPD data
county_map = county_map %>% mutate(
  County = County %>% as.character() %>% trim(),
  fac_county = pat_county,
  pat_county.name = County,
  fac_county.name = County
)
# add county names for patient
data_ = county_map %>% select(pat_county, pat_county.name) %>% 
  left_join(data_, ., by='pat_county')
# add rurality
data_ = urban_classif_reduced %>% 
  select(pat_county.name, pat_urban_rural) %>% 
  left_join(data_, ., by='pat_county.name')
# add county names for facility
data_ = county_map %>% select(fac_county, fac_county.name) %>% 
  left_join(data_, ., by='fac_county')
# add rurality
data_ = urban_classif_reduced %>% 
  select(fac_county.name, fac_urban_rural) %>% 
  left_join(data_, ., by='fac_county.name')
# setdiff(urban_classif_reduced$county, data_$fac_county)
# setdiff( urban_classif_reduced$county,  data_$fac_county.name)
# setdiff( data_$fac_county.name, urban_classif_reduced$county)


#
# __Subset OSHPD Hospital Utilization Variables----
#

# edited from 2018-11-12_Transfer RFS
# choose subset of variables to explore
hosp_factors = utilization %>% 
  slice(4:length(utilization$OSHPD_ID)) %>% 
  select(OSHPD_ID, FAC_NAME, COUNTY, FACILITY_LEVEL, 
         TYPE_CNTRL, TYPE_SVC_PRINCIPAL, ED_LIC_LEVL_BEGIN,
         TEACH_HOSP, PED_BED_LIC, NICU_BED_LIC, PSY_BED_LIC,
         # TRAUMA_CTR,  is the same as EMSA_TRAUMA_CTR_DESIG+EMSA TRAUMA PEDS
         EMSA_TRAUMA_CTR_DESIG, EMSA_TRAUMA_PEDS_CTR_DESIG, #starts_with('ED_'),
         EMS_AMB_DIVERS, HEALTH_SVC_AREA) %>% filter(!is.na(OSHPD_ID)) %>% 
  mutate(OSHPD_ID = OSHPD_ID %>% as.character() %>% as.integer()) %>%
  # get rid of excess categories in factor variables
  mutate_if(is.factor,funs(as.factor(as.character(.)))) %>%
  mutate(PED_BED_LIC = as.integer(as.character(PED_BED_LIC)),
         NICU_BED_LIC = as.integer(as.character(NICU_BED_LIC)),
         PSY_BED_LIC = as.integer(as.character(PSY_BED_LIC)),
         has_ped_bed_lic = PED_BED_LIC>0,
         has_nicu_bed_lic = NICU_BED_LIC>0,
         has_psy_bed_lic = PSY_BED_LIC>0,
         trauma_ctr = EMSA_TRAUMA_CTR_DESIG != 0,
         peds_trauma_ctr = EMSA_TRAUMA_PEDS_CTR_DESIG !=0,
         oshpd_id = OSHPD_ID %>% substring(4) %>% as.integer(),
         
         # replace 0 with NA
         EMSA_TRAUMA_CTR_DESIG2 = replace(
           EMSA_TRAUMA_CTR_DESIG, EMSA_TRAUMA_CTR_DESIG==0, NA),
         EMSA_TRAUMA_PEDS_CTR_DESIG2 = replace(
           EMSA_TRAUMA_PEDS_CTR_DESIG, EMSA_TRAUMA_PEDS_CTR_DESIG==0, NA),
         ED_LIC_LEVL_BEGIN2 = replace(
           ED_LIC_LEVL_BEGIN, ED_LIC_LEVL_BEGIN==0, NA),
         peds_trauma = EMSA_TRAUMA_PEDS_CTR_DESIG
  )
# hosp_factors$EMSA_TRAUMA_CTR_DESIG[hosp_factors$EMSA_TRAUMA_CTR_DESIG==0] = NA
# hosp_factors$EMSA_TRAUMA_PEDS_CTR_DESIG[hosp_factors$EMSA_TRAUMA_PEDS_CTR_DESIG==0] = NA
# hosp_factors$ED_LIC_LEVL_BEGIN[hosp_factors$ED_LIC_LEVL_BEGIN==0] = NA
# Standby < Basic < Comprehensive, set basic as reference since most common
hosp_factors$ed_level = hosp_factors$ED_LIC_LEVL_BEGIN2 %>% factor(
  levels=c('Basic', 'Standby', 'Comprehensive'))
hosp_factors = hosp_factors %>% select(-OSHPD_ID) %>% 
  # put 'hf.' as prefix to variables, make lower case
  setNames(paste0('hf.', names(.) %>% str_to_lower())) %>% 
  # put 'oshpd_id' first and then rest of variables
  rename(oshpd_id = hf.oshpd_id) %>% select(oshpd_id, everything())
# join with dataset 
data_ = left_join(data_, hosp_factors, by='oshpd_id')


#
# __ link Peds Ready data----
#

# edited from 2018-11-12_Transfer RFS
linked_pilot_reduced = linked_pilot %>% select(
  OSHPD_ID, Score,TotalEDPatients,
  PedEDPatientCat, PedEDPatients, EDConfig, PhysTraining_Ped_YN,
  PhysTraining_PedEM_YN, starts_with('Policies'), -Policies_Maltreat_YN, agreements, guidelines) %>% mutate(
    has_agreements = case_when(
      agreements=='Y'~TRUE,
      agreements=='N'~FALSE
    ),
    has_guidelines = case_when(
      guidelines=='Y'~TRUE,
      guidelines=='N'~FALSE    
    ),
    # agreements = recode(agreements,Y='Yes',N='No') %>% 
    #   factor(levels=c('Yes','No')),    
    agreements.filled = recode(agreements, Y='Yes',N='No', .default="Unknown") %>% 
      factor(levels=c('Yes','No','Unknown')),
    guidelines.filled = recode(guidelines, Y='Yes',N='No', .default="Unknown") %>% 
      factor(levels=c('Yes','No','Unknown')),
    Score_quintiles = case_when(
      Score<20~"0-20",
      Score>=20 & Score<40 ~ "20-40",
      Score>=40 & Score<60 ~ "40-60",
      Score>=60 & Score<80 ~ "60-80",
      Score>=80 ~ "80-100"
    ) %>% factor(levels=c('80-100', '60-80', '40-60', '20-40')),
    PedEDPatientCat = PedEDPatientCat %>% factor(levels=c('high', 'mediumHigh',
                                                          'medium', 'low'))
  ) %>% 
  rename (oshpd_id = OSHPD_ID) %>%
  mutate(oshpd_id = oshpd_id %>% substring(4) %>% as.integer())

# clean up an error
linked_pilot_reduced$EDConfig[linked_pilot_reduced$EDConfig=='GenEd']='GenED'
linked_pilot_reduced$EDConfig = linked_pilot_reduced$EDConfig %>% as.character() %>% as.factor()
linked_pilot_reduced = linked_pilot_reduced %>%
  # put 'hf.' as prefix to variables
  setNames(paste0('pr.', names(.))) %>% rename(oshpd_id = pr.oshpd_id)
# merge data
data_ = left_join(data_, linked_pilot_reduced, by='oshpd_id')
# subset to hospitals with peds-ready data
data_$pr.responded = !is.na(data_$pr.Score)
data_ = data_ %>% filter(pr.responded)
write_feather(data_, '2012-02-25_2012_all_peds.feather')

