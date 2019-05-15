# Goal: Extract 2012 individual data, similar to `2.pre-process_and_merge`
# except it doesn't include derivation of hospital-level data

#
# Set-up ----
#

# Import libraries
library(RPostgres) #database
library(tidyverse) # data munging, plotting etc
library(feather) # output
library(icdpicr) # for illness severity
library(lubridate) # for managing dates

# Parameters
odx_max = 10
run_injury_sev = TRUE

# Custom functions
source('custom_functions/2.complex_chronic_conditions.R')

# Import files
oshpd_folder = file.path(getwd(),'local_data','oshpd_full_data')


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

census.income = read.csv('data_dropbox/census_ACS/s1903_income/ACS_11_5YR_S1903_with_ann.csv')
census.edu = read.csv('data_dropbox/census_ACS/s1501_education/ACS_11_5YR_S1501_with_ann.csv')


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
  mutate(dx_prin = X.ICD.9.CM.CODE. %>% str_remove_all("\'") %>% trimws(), 
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
  County = County %>% as.character() %>% trimws(),
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




# Export data----
write_feather(data_, 'local_transfers_dev/2019-05-14_2012_all_peds.feather')


