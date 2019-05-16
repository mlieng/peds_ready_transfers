# Goal: Extract 2011 individual data to create hospital-level data

#
# Set-up ----
#

# Import libraries
library(RPostgres) #database
library(tidyverse) # data munging, plotting etc
library(feather) # output
library(icdpicr) # for illness severity
library(lubridate) # for managing dates
library(stringr) # for mananging strings

# Parameters
odx_max = 10
run_injury_sev = FALSE

# Custom functions
source('custom_functions/2.complex_chronic_conditions.R')

# Import files
oshpd_folder = file.path(getwd(),'local_data','oshpd_full_data')

# utilization = file.path('data_dropbox','oshpd_utilization',
#                         '2012_final_data_set_Hosp12_util_data_FINAL.csv')
# utilization = read.csv(utilization, header=TRUE, fileEncoding="UTF-8-BOM")
utilization = read.csv(file.path(
  'data_dropbox','oshpd_utilization',                             
  '2011_final_data_set_Hosp11_util_data_FINAL_sections1-4_combined.csv'),
  header = TRUE,  fileEncoding = 'UTF-8-BOM'
  )

financial = read.csv(file.path(
  'data_dropbox','oshpd_quarterly_financial',                             
  '2011_Q4_R4_CSV.csv'),
  header = TRUE,  fileEncoding = 'UTF-8-BOM'
  )


urban_classif = file.path(getwd(),'data_dropbox','cdc',
                          'NCHSURCodes2013.csv')
urban_classif = read.csv(urban_classif, header=TRUE, fileEncoding="UTF-8-BOM")
# county # -> county name
county_map = file.path(getwd(),'data_dropbox', '2019.02.14_county.csv')
county_map = read.csv(county_map, header=TRUE, fileEncoding="UTF-8-BOM")
# icd 9 cm -> 18 groups
ccs_multi_dx_tool = file.path(
  'data_dropbox', 'hcup_ahrq_ccs', 'Multi_Level_CCS_2015', 'ccs_multi_dx_tool_2015.csv')
ccs_multi_dx_tool = read.csv(ccs_multi_dx_tool, header=TRUE)
pecarn_illness_severity = read.csv(
  'data_dropbox/PECARN_DGS_severity/PECARN_ICD9_MASTER_to_illness_severity.csv')



peds_ready_pilot = read.csv(
  file.path('local_data','peds_ready',
    '2012_CA_PediatricReadinessPilotData.csv'), 
    header=TRUE)
# pr_to_oshpd_id = read.csv('data_dropbox/2018.05.21_hospital_id_map.csv', 
#                      header=TRUE, row.names=1)

pr_to_oshpd_id = read.csv(
  file.path('local_data',
    '2019-05-10_pedsready_oshpd_linkage2011.csv'), 
    header=TRUE)
cah_data = read.csv('data_dropbox/critical_access_hospitals/critical_access_hospitals.csv', 
                    header=TRUE, allowEscapes = TRUE) 
# cha_childrens = read.csv('data_dropbox/california_childrens_hospitals/california_childrens_hospitals.csv')
# 
# cha_childrens = read.csv(file.path(
#   'data_dropbox', 'california_childrens_hospitals',
#   'california_childrens_hospitals.csv'),
#   header = TRUE)

census.income = read.csv('data_dropbox/census_ACS/s1903_income/ACS_11_5YR_S1903_with_ann.csv')
census.edu = read.csv('data_dropbox/census_ACS/s1501_education/ACS_11_5YR_S1501_with_ann.csv')

# census.income = read.csv('data_dropbox/census_ACS/s1903_income/ACS_12_5YR_S1903_with_ann.csv')
# census.edu = read.csv('data_dropbox/census_ACS/s1501_education/ACS_12_5YR_S1501_with_ann.csv')


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
  filter(start_year==2011 & age<18) %>% 
  select(id, database, rln, birthdate, age, age_days, pat_zip, pat_county, sex,
         race_group, lang_spoken, start_date, end_date, start_day_of_week, 
         start_month, start_quarter, start_year, end_year, oshpd_id, 
         ed_disp, ip_source_site, ip_source_route, ip_source_license, ip_disp,
         payer, insurance, ccs_dx_prin, fac_county, 
         dx_prin, odx1, odx2, odx3, odx4, odx5, odx6, odx7, odx8, odx9, odx10,
         # odx11, odx12, odx13, odx14, odx15, odx16, odx17, odx18, odx19, odx20,
         # odx21, odx22, odx23, odx24,
         ec_prin, ec1, ec2, ec3, ec4) %>% 
  rename(sql_id = id) %>%
  collect()

end.time=Sys.time()
print(paste('Time Finished: ', end.time))
print(end.time-start.time)

#
# Individual Data: Post-process ----
#

#
# __Recode OSHPD data ----
#

data_  = encounters 

data_ %>% xtabs(~database+ ip_source_route, data=., addNA=TRUE)

# filter inpatient so that they came through the ed
data_ = data_ %>% filter(database=='edd' | (
  database=='pdd' & ip_source_route==1) & !is.na(ip_source_route))

# data_$ip_source_route %>% table(useNA = 'always')
data_ %>% xtabs(~database+ ip_source_route, data=., addNA=TRUE)

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
    
    ed_disp2 = case_when(
      ip_source_route==1~'admitted',
      TRUE~as.character(ed_disp)
    ),
    
    outcome = case_when(
      ed_disp %in% c(2,5) ~ 'transferred',
      ip_source_route==1 ~'admitted',
      ed_disp==1~'dc home'
    ),
    
    transferred = outcome=='transferred',
    transferred.factor = transferred %>% factor(levels=c('FALSE', 'TRUE')),
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
    
    lang_spoken3 = recode(
      lang_spoken, ENG='English', SPA='Spanish', .default='Other/Unknown') %>% 
    # specify order of factor
    factor(levels = c('English', 'Spanish','Other/Unknown')),
  
    # recode race group
    race_group2 = recode(
      race_group, `1`='White', `2`='Black', `3`='Hispanic', `4`='A/PI', 
      `5`='AI/AN', `6`='Other', `0`='Unknown') %>% 
      # specify order of factor
      factor(
        levels = c('Hispanic', 'White','Black','A/PI',
                   'Other','Unknown','AI/AN')),
 
    race_group2.5 = recode(
      race_group2,
      `White` = 'White',
      `Black` = 'Black',
      `Hispanic` = 'Hispanic/Latino',
      `A/PI` = 'Asian/Pacific Islander',
      .default = 'Other/Unknown'
      ) %>% factor(levels=c('White', 'Hispanic/Latino', 'Black', 'Asian/Pacific Islander',
                            'Other/Unknown')),#relevel('White')    
      
    race_group3 = recode(
      race_group2,
      `White` = 'White',
      `Black` = 'Black',
      `Hispanic` = 'Hispanic/Latino',
      #`A/PI` = 'Asian/Pacific Islander',
      .default = 'Other/Unknown'
      ) %>% factor(levels=c('White', 'Hispanic/Latino', 'Black', #'Asian/Pacific Islander',
                            'Other/Unknown')),#relevel('White')
    
    # copied from 2018-11-16 test descriptive
    sex2 = recode(
      sex, F='Female', M='Male') %>% factor(levels=c('Male', 'Female')),
    
    
    birthdate = ymd(birthdate),
    age_exact = (interval(birthdate, start_date))/years(1), # age at beginning of encounter
    
    # originally copied from 2018-11-16 test descriptive
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
        age>=1 & age<12 ~ "1y-11y",
        age>=12 & age<18 ~"12y-17y",
        age>=18 & age<21 ~NA_character_) %>% factor(
          levels=c('0-27d', '28d-12m', '1y-11y', 
                '12y-17y', '18-21y')
        ), #%>% relevel('12y-17y'),    
       
      age_cats3 = case_when(
        age_days<28 ~ "0-27d",
        age_days>=28 & age<1 ~ "28d-12m",
        age>=1 & age<18 ~"1y-17y",
        age>=18 & age<21 ~NA_character_) %>% factor(
          levels=c('0-27d','28d-12m','1y-17y')) ,#%>% relevel('1y-17y'),
    
    start_weekend = start_day_of_week %in% c('Sat', 'Sun'),
    start_month.factor = factor(start_month, levels=c(1:12) %>% as.character()),
    enc_season = case_when(
      start_month %in% c(12,1,2) ~ "Winter",
      start_month %in% c(3,4,5) ~ "Spring",
      start_month %in% c(6,7,8) ~ "Summer",
      start_month %in% c(9,10,11) ~ "Fall") %>% 
      factor(levels = c('Winter', 'Spring', 'Summer', 'Fall')),
    
    # ccs mental health #TODO double check if this is correct
   #  ccs_mental_health = ccs_dx_prin >= 650 & ccs_dx_prin <= 670
   
        insurance2.5 = recode(insurance, 
        `Private`='Private',
        `Public-Medicare-MediCal`='Public',
        `Self-Pay`='Uninsured/self-pay', #self-pay is the same as uninsured
        .default='Other'
        ) %>% factor(levels=c('Private', 'Public', 'Uninsured/self-pay',
                              'Other')),
                             
        # recode similar to Huang paper
        insurance2 =case_when(
          database=='pdd' & payer=='3'~'Private',
          database=='pdd' & payer=='2'~'Medicaid',
          database=='pdd' & payer=='8'~'Uninsured/self-pay',
          database=='pdd' & payer %in% c('4', '5', '6', '7', '9')~'Other',
          database=='pdd' & payer=='1'~'Medicare',
          database=='pdd' & payer=='9'~'Unknown',
          
          database=='edd' & payer %in% c('12','13','14','16','BL','CI','HM')~'Private',
          database=='edd' & payer=='MC'~'Medicaid',
          database=='edd' & payer=='09'~'Uninsured/self-pay',
          database=='edd' & payer %in% c('11','AM','CH','DS','OF', 'TV','VA','WC','00')~'Other',
          database=='edd' & payer %in% c('MA','MB')~'Medicare',
          database=='edd' & payer=='99'~'Unknown'          
        ) %>% factor(levels=c('Medicaid', 'Private', 'Uninsured/self-pay', 'Medicare','Other', 'Unknown')),
        insurance3 = recode(
          insurance2, 'Medicare'='Other', 'Unknown'=NA_character_) %>% 
          factor(levels=c('Medicaid', 'Private', 'Uninsured/self-pay','Other')
        ),
        has_medicare = insurance2 =='Medicare'
  )


#
# __Clinical Classifications Software (18 categories) ----
#

ccs = ccs_multi_dx_tool %>% 
  # clean up
  mutate(dx_prin = X.ICD.9.CM.CODE. %>% str_remove_all("\'") %>% str_trim(), 
         ccs_group = as.integer(X.CCS.LVL.1.),
         # combine 'Mental illness' with '
         ccs_label = X.CCS.LVL.1.LABEL. %>% str_replace('Mental illness', 'Mental Illness')) %>%
  select(dx_prin, ccs_group, ccs_label)

# translate principle diagnosis
data_ = left_join(data_, ccs, by='dx_prin')

# translate other diagnoses
orig_cols = c('dx_prin', 'ccs_group', 'ccs_label')
for(i in seq(1:odx_max)){
  new_cols = c(paste0('odx',i), paste0('ccs_group.',i), paste0('ccs_label.',i))
  data_ = ccs %>% 
    # rename columns to the appropriate number (e.g. dx_prin -> odx1, ccs_group --> ccs_group.1)
    select(orig_cols) %>% setNames(new_cols) %>% 
    left_join(data_, ., by=paste0('odx',i))
}

# count number of injury codes
df_has_injury = data_%>% select(starts_with('ccs_label')) %>% 
  mutate_all(~(.=='Injury and poisoning')) 
data_$ccs_injury_count = apply(df_has_injury, 1, function(x) sum(x, na.rm=TRUE))
remove(df_has_injury)

# convert counting
data_ = data_ %>% mutate(
  ccs_injury_any = ccs_injury_count >= 1, #replace_na(FALSE), unnecessary
  ccs_mental_health = ccs_label =='Mental Illness',
  ccs_injury = ccs_label == 'Injury and poisoning'
)



#
# __PECARN Illness Severity (Based on ICD9) (Alessandrini et al. 2012)----
#

illness_severity = pecarn_illness_severity %>% mutate(
  dx_prin = ICD9_Code %>% as.character(),
  sev.score = Severity_Score,
  # create variable where "other categories" -> NA (missing)
  sev.score.int = recode(
    Severity_Score,
    "1" = '1',
    "2" = '2',
    "3" = '3', 
    "4" = '4',
    "5" = '5',
    "Invalid: Additional digit required" = NA_character_,
    "Not Categorized" = NA_character_,
    "No code entered" = NA_character_
    ) %>% as.numeric(),
  sev.score.factor = sev.score %>% factor(levels=c('1','2','3','4','5')),
  sev.major_group = Major_Group_Desc,
  sev.subgroup = Subgroup_Desc
) %>% select(dx_prin, sev.score, sev.score.factor, sev.score.int, sev.major_group, sev.subgroup)

# join with data
data_ = left_join(data_, illness_severity, by='dx_prin')


# translate other diagnoses
orig_cols = c('dx_prin', 'sev.score.int')
for(i in seq(1:odx_max)){
  new_cols = c(paste0('odx',i), paste0('sev.score.int.',i))
  data_ = illness_severity %>% 
    # rename columns to the appropriate number (e.g. dx_prin -> odx1, sev.score.int --> sev.score.int.1)
    select(orig_cols) %>% setNames(new_cols) %>% 
    # merge with original data
    left_join(data_, ., by=paste0('odx',i))
}


# otherwise outputs an error if all are NA
my.max <- function(x) ifelse( !all(is.na(x)), max(x, na.rm=T), NA)

# calculate maximum illness severity
data_$sev.all.max.int = data_ %>% 
  select(starts_with('sev.score.int')) %>% 
  apply(1, my.max)

data_ = data_ %>% mutate(
  sev.all.max.factor = factor(sev.all.max.int, levels=c('1','2','3','4','5'))
)

#
# __Complex Chronic Conditions (Feudtner et al. 2014) ----
#

# Note: When doing PAT will need to use the four different categories and combine together


# data_ = data_ %>% select(dx_prin, starts_with('odx'))

# convert principle diagnosis
data_ = data_ %>% mutate(
  ccc_label = convert_icd9dx_TO_ccc(dx_prin),
  has_ccc = !is.na(ccc_label)
)

# convert other diagnoses
for(i in seq(1:odx_max)){
  data_[[paste0('ccc_label.',i)]] = convert_icd9dx_TO_ccc(data_[[paste0('odx',i)]])
}

# count number of injury codes
data_$ccc_count = data_ %>%
  select(starts_with('ccc_label')) %>%
  mutate_all(~(!is.na(.))) %>% # checks if there is a label for type
  # select(ccc_label, ccc_label.1, ccc_label.2) %>% 
  # calculates the sum
  apply(1, function(x) sum(x, na.rm=TRUE))

data_ = data_ %>% mutate(
  has_ccc_any = ccc_count >=1, # don't need replaceNA, if all are NA --> 0
  has_ccc_any.factor = has_ccc_any %>% factor(levels=c('FALSE', 'TRUE'))
)


#
# __Injury Severity Score (Clark 2018) ----
#
# https://github.com/ablack3/icdpicr, https://link.springer.com/article/10.1186/s40621-018-0149-8

old_cols = c('sql_id', 'dx_prin', paste0(rep('odx', odx_max), seq(1:odx_max))) #odx1, odx2...odx(n)
new_cols = c('sql_id', paste0(rep('dx', odx_max), seq(1:odx_max+1))) #dx1, dx2 ... dx(n+1)


# needs to be in the format 'id, dx1, dx2 ....dxn'

df_in = data_ %>% 
#  filter(ccs_injury_any) %>% 
  select(old_cols) %>% setNames(new_cols)

if(run_injury_sev){
  start.time = Sys.time()

  iss = df_in %>% icdpicr::cat_trauma('dx', icd10=FALSE)
  remove(df_in)
  
  end.time=Sys.time()
  print(paste('Time Finished: ', end.time))
  print(end.time-start.time)
  
  iss2 = iss %>% select(sql_id, maxais, riss, sev_1) %>% 
    rename(injury.sev = riss, 
           max.injury.sev=maxais,  # 9 = unknown
           injury.sev.dx_prin = sev_1 ) %>%  #9 = unknown) %>% 
    mutate(
      
      # recode into categories based on Huang 2017
      injury.sev.cat = case_when(
        injury.sev >=1 & injury.sev<=9 ~ 'Minor',
        injury.sev >= 10 ~ 'Moderate to severe'
      ), # can go from 0 ---> 100
      
      
      # maximum injury severity
      max.injury.sev2 = case_when(
        max.injury.sev==9 ~ 'Unknown',
        is.na(max.injury.sev) ~ 'Missing/Not Assigned',
        TRUE ~ as.character(max.injury.sev)
      ),  # 2 types of missing
      max.injury.sev.factor = max.injury.sev %>% 
        factor(levels=c(1,2,3,4,5,6,9), 
                  labels=c('Minor', 'Moderate', 'Serious', 'Severe', 'Critical', 'Unsurvivable', 'Unknown')) %>% 
        relevel(1), # drops 0
      
      max.injury.sev.int = max.injury.sev %>% replace(max.injury.sev==9, NA) %>% as.numeric(),
     
      # injury severity based on principle diagnosis
      injury.sev.dx_prin2 = case_when(
        injury.sev.dx_prin==9 ~ 'Unknown',
        is.na(injury.sev.dx_prin) ~ 'Missing/Not Assigned',
        TRUE ~ as.character(injury.sev.dx_prin)
      ),  # 2 types of missing
      injury.sev.dx_prin.factor = injury.sev.dx_prin %>% 
        factor(levels=c(1,2,3,4,5,6,9), 
                  labels=c('Minor', 'Moderate', 'Serious', 'Severe', 'Critical', 'Unsurvivable', 'Unknown')) %>% 
        relevel(1), # drops 0
      
      injury.sev.dx_prin.int = injury.sev.dx_prin %>% replace(injury.sev.dx_prin==9, NA) %>% as.numeric()
       
  )

  # join data
  data_ = left_join(data_, iss2, by='sql_id')
  remove(iss)  
  
  
} else {message('Did not run injury severity score algorithm')}






#
# __Census Data (American FactFinder)----
#

# Income Data

# set 1st row as labels and delete
census.income.labels = census.income[1,] %>% mutate_all(as.character)
Hmisc::label(census.income) = census.income.labels
census.income = census.income %>% slice(2:nrow(census.income))

census.income.subset = census.income %>% 
  select(GEO.id2,HC02_EST_VC02) %>%
  # rename to human readable
  rename(
    census.zcta = GEO.id2,
    census.med.income = HC02_EST_VC02)  %>% 
  mutate(
    census.zcta = census.zcta %>% as.character %>% as.numeric,
    census.med.income = census.med.income %>% 
      as.character() %>% 
      str_replace_all(
        c('2,500-' = '2500',
          '250,000\\+' = '250000', 
           '-' = NA_character_ )) %>%  
      as.numeric()
    ) 

#
# Education Data
#

# set 1st row as labels and delete
census.edu.labels = census.edu[1,] %>% mutate_all(as.character)
Hmisc::label(census.edu) = census.edu.labels
census.edu = census.edu %>% slice(2:nrow(census.edu))

# subset and rename
census.edu.subset = census.edu %>% 
  select(GEO.id2, HC01_EST_VC16, HC01_EST_VC17) %>%
  # rename to human-human readable
  rename(
    census.zcta=GEO.id2, 
    census.hs_higher=HC01_EST_VC16,
    census.bach_higher=HC01_EST_VC17
    ) %>%
  mutate(
    census.hs_higher = replace(census.hs_higher, census.hs_higher=='-', NA),
    census.bach_higher = replace(census.bach_higher, census.bach_higher=='-', NA)
  ) %>% 
  # convert all to numeric
  mutate_if(is.factor, ~as.numeric(as.character(.)))

census.data = full_join(census.income.subset, census.edu.subset, by='census.zcta') %>% 
  filter(!is.na(census.zcta)) %>% mutate(pat_zip = as.character(census.zcta))

data_ = left_join(data_, census.data, by='pat_zip')

# Remove extra data sets
remove(list=c('census.income', 'census.income.labels', 'census.income.subset',
             'census.edu', 'census.edu.labels', 'census.edu.subset'))




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
         urban_rural.factor2 = case_when(
           #X2013.code==5~4,
           X2013.code==6~5,
           TRUE~as.numeric(X2013.code)
         ) %>% factor(levels=c(1,2,3,4,5), 
                  labels=c('Large central metro', 'Large fringe metro', 
                           'Medium metro', 'Small metro', 'Non metro'))
         )

# recode OSHPD data from numbers to county names (e.g. 58 -> Yuba)
county_map = county_map %>% mutate(
  County = County %>% as.character() %>% str_trim(),
  fac_county = pat_county,
  pat_county.name = County,
  fac_county.name = County
)

# add county names for patient
data_ = county_map %>% select(pat_county, pat_county.name) %>% 
  left_join(data_, ., by='pat_county')

# add rurality
data_ = urban_classif_reduced %>% 
  rename(nchs.pat_urban_rural = urban_rural.factor, 
         nchs.pat_urban_rural2 = urban_rural.factor2,
         nchs.pat_urban_rural.int = urban_rural.int) %>% 
  select(pat_county.name, nchs.pat_urban_rural, nchs.pat_urban_rural2,
         nchs.pat_urban_rural.int) %>% 
  left_join(data_, ., by='pat_county.name')



#
# -Hospital Data: Post-process----
#

#
# __OSHPD Hospital Utilization----
#


# edited from 2018-11-12_Transfer RFS
hosp_factors = utilization %>% 
  slice(4:length(utilization$OSHPD_ID)) %>% 
  select(OSHPD_ID, FAC_NAME, COUNTY, FACILITY_LEVEL, 
         TYPE_CNTRL, TYPE_SVC_PRINCIPAL, ED_LIC_LEVL_END,
         TEACH_HOSP, PED_BED_LIC, NICU_BED_LIC, PSY_BED_LIC,
         # TRAUMA_CTR,  is the same as EMSA_TRAUMA_CTR_DESIG+EMSA TRAUMA PEDS
         EMSA_TRAUMA_CTR_DESIG, EMSA_TRAUMA_PEDS_CTR_DESIG, #starts_with('ED_'),
         EMS_AMB_DIVERS, HEALTH_SVC_AREA,
         LONGITUDE, LATITUDE
         ) %>% filter(!is.na(OSHPD_ID)) %>% 
  mutate(OSHPD_ID = OSHPD_ID %>% as.character() %>% as.integer()) %>%
  # get rid of excess categories in factor variables
  mutate_if(is.factor, ~as.factor(as.character(.))) %>%
  mutate(PED_BED_LIC = as.integer(as.character(PED_BED_LIC)),
         NICU_BED_LIC = as.integer(as.character(NICU_BED_LIC)),
         PSY_BED_LIC = as.integer(as.character(PSY_BED_LIC)),
         has_ped_bed_lic = PED_BED_LIC>0,
         has_nicu_bed_lic = NICU_BED_LIC>0,
         has_psy_bed_lic = PSY_BED_LIC>0,
         trauma_ctr = EMSA_TRAUMA_CTR_DESIG != 0,
         peds_trauma_ctr = EMSA_TRAUMA_PEDS_CTR_DESIG !=0,
         # remove first 3 nubmers in OSHPD_ID (e.g. '106')
         OSHPD_ID = OSHPD_ID %>% substring(4) %>% as.integer(),
         
         # replace 0 with NA
         EMSA_TRAUMA_CTR_DESIG2 = replace(
           EMSA_TRAUMA_CTR_DESIG, EMSA_TRAUMA_CTR_DESIG==0, NA),
         EMSA_TRAUMA_PEDS_CTR_DESIG2 = replace(
           EMSA_TRAUMA_PEDS_CTR_DESIG, EMSA_TRAUMA_PEDS_CTR_DESIG==0, NA),
         ED_LIC_LEVL_END2 = replace(
           ED_LIC_LEVL_END, ED_LIC_LEVL_END==0, NA) %>% 
           # Standby < Basic < Comprehensive, set basic as reference since most common
           factor(levels=c('Basic', 'Standby', 'Comprehensive')),
         
        # correct spelling miscategorizations                   
        # TEACH_HOSP2 = TEACH_HOSP %>% recode(YES = 'YES', NO = 'NO', No = 'NO'),
         TEACH_HOSP = TEACH_HOSP %>% recode(YES = TRUE, NO = FALSE, No = FALSE),
        
        # change type for merge with rural-urban designations
        COUNTY = as.character(COUNTY)
  ) %>% 
  # put 'hf.' as prefix to variables, make lower case
  setNames(paste0('hf.', names(.) %>% str_to_lower())) %>% 
  # put 'oshpd_id' first and then rest of variables
  rename(oshpd_id = hf.oshpd_id) %>% select(oshpd_id, everything())

#
# __OSHPD Hospital Quarterly Financial ----
#

# Developed in 2019-05-07 Bivariate 2.5
financial_subset = financial  %>% filter(!is.na(FAC.NO)) %>% 
  select(FAC.NO, TEACH.RURL) %>% 
  mutate(
    oshpd_id = FAC.NO %>% as.character() %>% substring(4) %>% as.integer(),
    TEACH_RURL = case_when(
      TEACH.RURL=='R'~"Small/Rural",
      TEACH.RURL=='T'~"Teaching",
      TEACH.RURL==TRUE~NA_character_
      ),
    fin.small_rural = TEACH_RURL=='Small/Rural', 
    fin.teaching = TEACH_RURL=='Teaching') %>% select(-FAC.NO, -TEACH.RURL)

hosp_factors  = hosp_factors %>% left_join(financial_subset, by='oshpd_id') %>% 
  mutate(
    fin.small_rural = replace_na(fin.small_rural, FALSE),
    fin.teaching = replace_na(fin.teaching, FALSE)
  )

#
# __Other hospital designations ----
#

# Critical Access hospitals
cah_hospitals = cah_data$oshpd_id %>% as.character() %>% substring(4) %>% as.integer()
hosp_factors$cah = hosp_factors$oshpd_id %in% cah_data$oshpd_id

# Affiliation/Designation as indicated by California Children's Services
# remove '106' prefix from OSHPD_ID
childrens_hospitals = c(
  106010776, 106010856, 106190170, 106190429, 106190555, 106191227, 106191228, 
  106196168, 106204019, 106300032, 106314024, 106341006, 106341052, 106361246, 
  106370673, 106380777, 106380964, 106381154, 106430883, 106434040, 106434153, 
  106190796) %>% as.character() %>% substring(4) %>% as.integer()

hosp_factors = hosp_factors %>% mutate(
  ccs.childrens_hosp = oshpd_id %in% childrens_hospitals
)

(childrens = hosp_factors %>% filter(ccs.childrens_hosp) %>% select(
  oshpd_id, hf.fac_name, hf.ped_bed_lic, hf.nicu_bed_lic, 
  hf.emsa_trauma_peds_ctr_desig, hf.facility_level, 
  hf.type_svc_principal, hf.county))

# Add rurality
hosp_factors = urban_classif_reduced %>% 
  rename(nchs.fac_urban_rural = urban_rural.factor, 
         nchs.fac_urban_rural2 = urban_rural.factor2,
         nchs.fac_urban_rural.int = urban_rural.int) %>% 
  select(fac_county.name, nchs.fac_urban_rural, nchs.fac_urban_rural2,
         nchs.fac_urban_rural.int) %>% 
  rename(hf.county = fac_county.name) %>% 
  left_join(hosp_factors, ., by='hf.county')




#
# __Individual-level Aggregates: All ----
#

# hf.enc_total = hf.ALL_total
# pediatric_encounters = data_ %>% count(oshpd_id) %>% rename(hf.enc_total=n)
# hosp_factors = left_join(hosp_factors, pediatric_encounters, by = 'oshpd_id')

# add percent medicaid, percent medicaid or uninsured

percent_medicaid = data_ %>% 
  select(oshpd_id, insurance3) %>% group_by(oshpd_id) %>% 
  summarise(freq = sum(insurance3=='Medicaid', na.rm=TRUE),
            total = n()) %>% 
  mutate(rf = freq/total) %>% select(oshpd_id, rf) %>% 
  rename(hf.medicaid_rf = rf)

percent_medicaid_uninsured = data_ %>% 
  select(oshpd_id, insurance3) %>% group_by(oshpd_id) %>% 
  summarise(freq = sum(insurance3=='Medicaid' | insurance3=='Uninsured/self-pay', na.rm=TRUE),
            total = n()) %>% 
  mutate(rf = freq/total) %>% select(oshpd_id, rf) %>% 
  rename(hf.medicaid_or_uninsured_rf = rf)

# percent non-white
percent_nonwhite = data_ %>% 
  select(oshpd_id, race_group2) %>% group_by(oshpd_id) %>% 
  summarise(freq = sum(race_group2!='White', na.rm=TRUE),
            total = n()) %>% 
  mutate(rf = freq/total) %>% select(oshpd_id, rf) %>% 
  rename(hf.nonwhite_rf = rf)

# percent high severity

percent_sev4or5 = data_ %>% 
  select(oshpd_id, sev.all.max.int) %>% group_by(oshpd_id) %>% 
  summarise(freq = sum(sev.all.max.int==4|sev.all.max.int==5, na.rm=TRUE),
            total = n()) %>% 
  mutate(rf = freq/total) %>% select(oshpd_id, rf) %>% 
  rename(hf.sev4or5 = rf)

# join all

hosp_factors = hosp_factors %>% 
  left_join(percent_medicaid, by='oshpd_id')  %>% 
  left_join(percent_medicaid_uninsured, by='oshpd_id') %>% 
  left_join(percent_nonwhite, by='oshpd_id') %>%
  left_join(percent_sev4or5, by='oshpd_id')

# count transfers

# both admit and discharge home as control
both_control = data_ %>% 
  select(oshpd_id, transferred) %>% group_by(oshpd_id) %>% 
  summarise(hf.ALL_total=n(), 
            hf.ALL_transferred_total = sum(transferred, na.rm=TRUE)) %>% 
  ungroup %>% 
  mutate(hf.ALL_transferred_rf = hf.ALL_transferred_total/hf.ALL_total) 

# admit as control
admit_only = data_ %>% filter(outcome!='dc home') %>% 
  select(oshpd_id, transferred) %>% group_by(oshpd_id) %>% 
  summarise(hf.ADMIT_total=n(), 
            hf.ADMIT_transferred_total = sum(transferred, na.rm=TRUE)) %>% 
  ungroup %>% 
  mutate( hf.ADMIT_transferred_rf = hf.ADMIT_transferred_total/hf.ADMIT_total) 

# discharge home as control

dc_only = data_  %>% filter(outcome!='admitted') %>% 
  select(oshpd_id, transferred) %>% group_by(oshpd_id) %>% 
  summarise(hf.DC_total=n(), 
            hf.DC_transferred_total = sum(transferred, na.rm=TRUE)) %>% 
  ungroup %>% 
  mutate(hf.DC_transferred_rf = hf.DC_transferred_total/hf.DC_total) 

# join all

hosp_factors = hosp_factors %>% 
  left_join(both_control, by='oshpd_id')  %>% 
  left_join(admit_only, by='oshpd_id') %>% 
  left_join(dc_only, by='oshpd_id')



#
# __Individual-level Aggregates: Subset to non-injured ----
#

df_ = data_ %>% 
  #exclude rare events
  filter(!is.na(outcome)) %>% 
  filter(!ccs_injury_any) %>% 
  # exclude medicare 
  filter(insurance2!='Medicare') %>% mutate(
    transferred.factor = transferred %>% factor(levels=c('FALSE', 'TRUE'))
  ) 


percent_medicaid = df_ %>% 
  select(oshpd_id, insurance3) %>% group_by(oshpd_id) %>% 
  summarise(freq = sum(insurance3=='Medicaid', na.rm=TRUE),
            total = n()) %>% 
  mutate(rf = freq/total) %>% select(oshpd_id, rf) %>% 
  rename(hf.ei.medicaid_rf = rf)

percent_medicaid_uninsured = df_ %>% 
  select(oshpd_id, insurance3) %>% group_by(oshpd_id) %>% 
  summarise(freq = sum(insurance3=='Medicaid' | insurance3=='Uninsured/self-pay', na.rm=TRUE),
            total = n()) %>% 
  mutate(rf = freq/total) %>% select(oshpd_id, rf) %>% 
  rename(hf.ei.medicaid_or_uninsured_rf = rf)

# percent non-white
percent_nonwhite = df_ %>% 
  select(oshpd_id, race_group2) %>% group_by(oshpd_id) %>% 
  summarise(freq = sum(race_group2!='White', na.rm=TRUE),
            total = n()) %>% 
  mutate(rf = freq/total) %>% select(oshpd_id, rf) %>% 
  rename(hf.ei.nonwhite_rf = rf)

# percent high severity

percent_sev4or5 = df_ %>% 
  select(oshpd_id, sev.all.max.int) %>% group_by(oshpd_id) %>% 
  summarise(freq = sum(sev.all.max.int==4|sev.all.max.int==5, na.rm=TRUE),
            total = n()) %>% 
  mutate(rf = freq/total) %>% select(oshpd_id, rf) %>% 
  rename(hf.ei.sev4or5 = rf)

# join all

hosp_factors = hosp_factors %>% 
  left_join(percent_medicaid, by='oshpd_id')  %>% 
  left_join(percent_medicaid_uninsured, by='oshpd_id') %>% 
  left_join(percent_nonwhite, by='oshpd_id') %>%
  left_join(percent_sev4or5, by='oshpd_id')

# count transfers

# both admit and discharge home as control
both_control = df_ %>% 
  select(oshpd_id, transferred) %>% group_by(oshpd_id) %>% 
  summarise(hf.ei.ALL_total=n(), 
            hf.ei.ALL_transferred_total = sum(transferred, na.rm=TRUE)) %>% 
  ungroup %>% 
  mutate(hf.ei.ALL_transferred_rf = hf.ei.ALL_transferred_total/hf.ei.ALL_total) 

# admit as control
admit_only = df_ %>% filter(outcome!='dc home') %>% 
  select(oshpd_id, transferred) %>% group_by(oshpd_id) %>% 
  summarise(hf.ei.ADMIT_total=n(), 
            hf.ei.ADMIT_transferred_total = sum(transferred, na.rm=TRUE)) %>% 
  ungroup %>% 
  mutate( hf.ei.ADMIT_transferred_rf = hf.ei.ADMIT_transferred_total/hf.ei.ADMIT_total) 

# discharge home as control

dc_only = df_  %>% filter(outcome!='admitted') %>% 
  select(oshpd_id, transferred) %>% group_by(oshpd_id) %>% 
  summarise(hf.ei.DC_total=n(), 
            hf.ei.DC_transferred_total = sum(transferred, na.rm=TRUE)) %>% 
  ungroup %>% 
  mutate(hf.ei.DC_transferred_rf = hf.ei.DC_transferred_total/hf.ei.DC_total) 

# join all

hosp_factors = hosp_factors %>% 
  left_join(both_control, by='oshpd_id')  %>% 
  left_join(admit_only, by='oshpd_id') %>% 
  left_join(dc_only, by='oshpd_id')


#
# __ Link Peds Ready data----
#

# edited from 2018-11-12_Transfer RFs, updated 2019/05/14

# ADD OSHPD_IDs
peds_ready_pilot_reduced = peds_ready_pilot %>% 
  left_join(pr_to_oshpd_id %>% select(-ResponseID), by='portalID') %>%
  select(oshpd_id, everything()) 

peds_ready_pilot_reduced = peds_ready_pilot_reduced %>% select(
  oshpd_id, Score,TotalEDPatients,
  PedEDPatientCat, PedEDPatients, EDConfig, PhysTraining_Ped_YN,
  PhysTraining_PedEM_YN, starts_with('Policies'), -Policies_Maltreat_YN, agreements, guidelines) %>% mutate(
    guidelines = recode(guidelines, Y='Yes',N='No', .default="Unknown") %>% 
      factor(levels=c('Yes','No','Unknown')),
    agreements = recode(agreements, Y='Yes',N='No', .default="Unknown") %>% 
      factor(levels=c('Yes','No','Unknown')),
    guidelines2 = case_when(
      guidelines=='Yes'~TRUE,
      guidelines=='No'~FALSE
    ),

    agreements2 = case_when(
      agreements=='Yes'~TRUE,
      agreements=='No'~FALSE
    ),
    guidelines_agreements = agreements %>% as.character() %>% 
      replace(guidelines=='No', 'No Guidelines')%>% recode(
      'Yes' = 'Guidelines and Agreements', 
      'No' = 'Guidelines, No Agreements',
      'No Guidelines' = 'No Guidelines'),
    guidelines_agreements.factor = guidelines_agreements %>% 
      factor(levels=c('Guidelines and Agreements',
                      'Guidelines, No Agreements', 
                      'No Guidelines')),
    Score10 = Score/10,
    Score_quintiles = case_when(
      Score<20~"0-20",
      Score>=20 & Score<40 ~ "20-40",
      Score>=40 & Score<60 ~ "40-60",
      Score>=60 & Score<80 ~ "60-80",
      Score>=80 ~ "80-100"
    ) %>% factor(levels=c('80-100', '60-80', '40-60', '20-40')),
    PedEDPatientCat = PedEDPatientCat %>% 
      factor(levels=c('high', 'mediumHigh', 'medium', 'low'))) %>%
    mutate(oshpd_id = oshpd_id %>% substring(4) %>% as.integer())
    

# clean up an error
peds_ready_pilot_reduced$EDConfig[peds_ready_pilot_reduced$EDConfig=='GenEd']='GenED'
peds_ready_pilot_reduced$EDConfig = peds_ready_pilot_reduced$EDConfig %>% as.character() %>% as.factor()

peds_ready_pilot_reduced = peds_ready_pilot_reduced %>%
  # put 'hf.' as prefix to variables
  setNames(paste0('pr.', names(.))) %>% rename(oshpd_id = pr.oshpd_id)

# merge data
hosp_factors = hosp_factors %>% left_join(peds_ready_pilot_reduced, by='oshpd_id') %>% 
  mutate(pr.responded = !is.na(pr.Score))

# Export data----
write_feather(data_, 'local_transfers_dev/2019-05-14_2011_all_peds.feather')
write_feather(hosp_factors, 'local_transfers_dev/2019-05-14_2011_hospital_data.feather')

