
#
# Set-up ----
#

export_path = '2019-07-16_tables_for_draft2'

source('format_result_for_forest.R')
source('2.complex_chronic_conditions.R')

library(tidyverse) # includes 
# dplyr(data munging), ggplot2 (plotting), tidyr (tidy data)
library(knitr) # notebook

# library(formattable)
# library(DT)

# plotting
# library(ggplot2)
# library(forestplot)
# library(plotly)
# library(ggridges) # multiple distributions
# library(GGally) #ggpairs

# comparing
library(compareGroups)
library(skimr) # descriptive statistics

# storage
library(feather)
library(foreign) # EXPORT TO STATA

opts_chunk$set(tidy.opts=list(width.cutoff=60),tidy=FALSE, fig.align="center")
oshpd_folder = file.path(getwd(),'local_data','oshpd_full_data')

sessionInfo()

# Read data
data_ = read_feather('2019-05-14_2012_all_peds.feather')
hosp_factors  = read_feather('2019-05-14_2011_hospital_data.feather')


# Custom functions

univar_table = function(df, col){
  df %>% select_at(col) %>% group_by_at(col) %>% summarise(Freq=n()) %>% ungroup() %>%mutate(rel.freq = Freq/sum(Freq, na.rm=TRUE)) %>% arrange(desc(Freq))
}


getmode <- function(v) {
  uniqv <- unique(v)
  uniqv[which.max(tabulate(match(v, uniqv)))]
}

format_results = function(results){
  results %>% mutate(
    estimate = round(estimate,4), 
    std.error = round(estimate,4), 
    conf.int = paste0('(', round(conf.low,3),'-',round(conf.high,3),')'),
    p.value = formatC(p.value, format = "e", digits = 2) #format.pval(p.value, digits = 3)
  ) %>%  
    select(term, estimate, conf.int, std.error, p.value, -statistic, -conf.low, -conf.high)
  
}

make_forest_plot = function(tab){
  tab %>% 
    select(Label, estimate_label, conf.label, p.value_label) %>% 
    rbind(c('Variable',  'Adj. OR', '95% CI', 'p-value'),.) %>%
    forestplot(., 
               mean = c(NA, tab$estimate),
               lower = c(NA, tab$conf.low),
               upper = c(NA, tab$conf.high),
               xlog=TRUE,  # convert x-axis to log
               zero=1, #middle value = 1,
               graph.pos=3,
               xticks = c(0.66,0.8, 1,1.25, 1.5),
               boxsize=0.2, 
               ci.vertices=TRUE, ci.vertices.height = 0.4 # add whiskers
    )
}

# gen_table_results = function(res){
#   try(createTable(res) %>% print())
#   try(missingTable(res) %>% print())
# }

gen_table_results = function(res, missing_table = TRUE, addmd = TRUE, ...){
  
  table_ = createTable(res, ...)
  results = list(table = table_, 
                 if(missing_table) missing = try(missingTable(res))
                 )
  if(addmd) export2md(results$table)
  if(!addmd) print(results)
  
  return(results)
}


#
# Start timer---
#

start.time = Sys.time()

#
# Recodings ----
#

df_ = data_ %>% 
  #exclude rare events
  filter(!is.na(outcome)) %>% 
  filter(!ccs_injury_any) %>% 
  # exclude medicare 
  filter(insurance2!='Medicare') %>% mutate(
    transferred.factor = transferred %>% factor(levels=c('FALSE', 'TRUE'))
  ) 

df_hosp = hosp_factors %>% mutate(
  hf.medicaid_pct =  hf.medicaid_rf * 100,
  hf.medicaid_or_uninsured_pct = hf.medicaid_or_uninsured_rf * 100,
  hf.nonwhite_pct = hf.nonwhite_rf * 100,
  hf.sev4or5_pct = hf.sev4or5*100,
  
  hf.ALL_total_norm = scale(hf.ALL_total),
  hf.medicaid_norm = scale(hf.medicaid_rf),
  hf.nonwhite_norm = scale(hf.nonwhite_rf),
  hf.sev4or5_norm = scale(hf.sev4or5),
  
  pr.score_cut_70 = pr.Score > 70,
  pr.score_cut_70_factor = pr.score_cut_70 %>% factor(levels=c('FALSE', 'TRUE')),
  pr.guidelines_agreements2 = recode(pr.guidelines_agreements,
                                     "Guidelines and Agreements" = 'TRUE',
                                     "Guidelines, No Agreements" = 'FALSE',
                                     "No Guidelines" = 'FALSE') %>% factor(levels=c('TRUE', 'FALSE')),
  
  fin.small_rural_factor = fin.small_rural %>% factor(levels=c('TRUE', 'FALSE')),
  
  hf.type_cntrl2 = hf.type_cntrl %>% as.character() %>% recode(
    "City and/or County" = 'Non-profit',
    "District" = "Non-profit",
    "Investor - individual" = 'Investor',
    "Investor - Corporation" = 'Investor',
    "Investor - Limited Liability Company" = 'Investor',
    "Investor - Partnership" = 'Investor',
    "Non-Profit Corporation (incl. Church-related)" = 'Non-profit', 
    "University of California" = 'Non-profit', 
    "State" = 'Non-profit',
    .default = NA_character_, # 'Unknown',
    .missing = NA_character_ #'Unknown'
    # "" = 'Unknown',
    # "0" = 'Unknown',
    
  ) %>% factor(levels=c('Non-profit', 'Investor', 'Unknown')),
  
  hf.emsa_trauma_ctr_desig3 = hf.emsa_trauma_ctr_desig %>% as.character() %>% 
    recode(
      "LEVEL I" = 'Level I & II', 
      "LEVEL II" = 'Level I & II', 
      "LEVEL III" = 'Level III & IV',
      "LEVEL IV" = 'Level III & IV', 
      "0" = 'None',
      .default = NA_character_ ,
      .missing = NA_character_
    ) %>% factor(levels=c('Level I & II', 'Level III & IV', 'None')),
  hf.emsa_trauma_peds_ctr_desig3 = hf.emsa_trauma_peds_ctr_desig %>% as.character() %>% 
    recode(
      "LEVEL I PEDS" = 'TRUE', 
      "LEVEL II PEDS" = 'TRUE', 
      "0" = 'FALSE',
      .default = NA_character_ ,
      .missing = NA_character_
    ) %>% factor(levels=c('FALSE', 'TRUE'))
)

df_  = df_ %>% mutate(
  
  age_cats4 = case_when(
    age_days<28 ~ "0-27d",
    age_days>=28 & age_exact<1 ~ "28d-12m",
    age_exact>=1 & age_exact<2 ~"13m-24m",
    age_exact>=2 & age_exact<6 ~"2y-5y",
    age_exact>=6 & age_exact<12 ~"6y-11y",
    age_exact>=12 & age_exact<18 ~"12y-17y",
    age_exact>=18 & age_exact<21 ~NA_character_) %>% factor(
      levels=c('0-27d','28d-12m','13m-24m','2y-5y','6y-11y','12y-17y')),
  age_cats4.1 = case_when(
    age_days<28 ~ "Neonate",
    age_days>=28 & age_exact<1 ~ "Infant",
    age>=1 & age<2 ~"Toddler",
    age>=2 & age<6 ~"Early Childhood",
    age>=6 & age<12 ~"Middle Childhood",
    age>=12 & age<18 ~"Early Adolescence",
    age>=18 & age<21 ~NA_character_) %>% factor(
      levels=c("Neonate", "Infant", "Toddler", "Early Childhood", "Middle Childhood", "Early Adolescence"))
)


#
# Join individual and hospital ----
#


# subset hospital data to data in dataset
hospitals_in_data = df_$oshpd_id %>% unique()
df_hosp = df_hosp %>% filter(oshpd_id %in% hospitals_in_data) 

# Check % going to exclude
df_hosp %>% dim()
df_hosp %>% univar_table('pr.responded')
# df_hosp %>% univar_table('ccs.childrens_hosp')

# join with dataset 
df_ = left_join(df_, df_hosp, by='oshpd_id')
df_ %>% dim()

df_ %>% univar_table('pr.responded')
# df_ %>% univar_table('ccs.childrens_hosp')

# Exclude children's hospitals and missing Peds Ready data
df_$pr.responded = !is.na(df_$pr.Score)
df_ = df_ %>% filter(pr.responded) #%>% filter(!cha.childrens_hosp)
df_ %>% dim()

df_hosp = df_hosp %>% filter(pr.responded) # %>% filter(!cha.childrens_hosp)
df_hosp %>% dim()

df_ %>% univar_table('outcome')
df_ %>% univar_table('transferred')

df_hosp %>% univar_table('fin.small_rural')
df_ %>% group_by(fin.small_rural, outcome) %>% summarise(freq=n()) %>% ungroup()%>% 
  group_by(fin.small_rural) %>% mutate(rel.freq=freq/sum(freq))



#
# Hospital- Univariate & Bivariate ----
#


message('Univariate & Bivariate: Hospital')
hosp_vars_other = c(
  'nchs.fac_urban_rural', 
  'nchs.fac_urban_rural2', 
  'nchs.fac_urban_rural.int', 
  'ccs.childrens_hosp',
  'hf.facility_level', 
  'hf.type_cntrl', # non-profit,
  'hf.type_cntrl2', # non-profit
  'hf.type_svc_principal', #gen med surg, pediatric, psychiatric etc
  'hf.teach_hosp', 
  'fin.teaching',
  'fin.small_rural',
  # 'hf.health_svc_area',
  'hf.ped_bed_lic', 'hf.has_ped_bed_lic', 
  # hf.nicu_bed_lic', 'hf.has_nicu_bed_lic',
  # hf.psy_bid_lic', 'hf.has_psy_bed_lic',
  'hf.trauma_ctr', 
  'hf.emsa_trauma_ctr_desig', 'hf.emsa_trauma_ctr_desig2', 
  'hf.emsa_trauma_ctr_desig3', 
  'hf.emsa_trauma_peds_ctr_desig', 'hf.emsa_trauma_peds_ctr_desig2', 
  'hf.emsa_trauma_peds_ctr_desig3', 
  'hf.ed_lic_levl_end', 'hf.ed_lic_levl_end2', 
  'hf.ems_amb_divers',
  'hf.medicaid_rf', 'hf.medicaid_pct',
  'hf.medicaid_or_uninsured_rf', 'hf.medicaid_or_uninsured_pct',
  'hf.nonwhite_rf', 'hf.nonwhite_pct',
  'hf.sev4or5', 'hf.sev4or5_pct',
  'hf.ALL_total',
  'hf.ei.ALL_total',
  'hf.ALL_total_norm',
  'hf.medicaid_norm',
  'hf.nonwhite_norm',
  'hf.sev4or5_norm',
  
  'fin.small_rural_factor',
  'pr.Score', 'pr.Score10',
  'pr.score_cut_70_factor',
  'pr.TotalEDPatients', 'pr.PedEDPatients',
  'pr.guidelines2',
  'pr.agreements2',
  'pr.guidelines_agreements', 'pr.guidelines_agreements2'
  
)

df_hosp %>% select(hosp_vars_other) %>% skim()

res1.1 = compareGroups(
  ~.-fin.small_rural_factor, 
  data=df_hosp %>% select(hosp_vars_other),
  method = c(
    nchs.fac_urban_rural.int = NA,
    hf.ped_bed_lic = NA,
    ccs.childrens_hosp = 3,
    hf.has_ped_bed_lic = 3, 
    hf.teach_hosp = 3,
    fin.teaching = 3,
    fin.small_rural = 3, 
    hf.ped_bed_lic = NA,
    hf.trauma_ctr = 3,
    hf.medicaid_rf = NA,
    hf.medicaid_pct = NA,
    hf.medicaid_or_uninsured_rf = NA,
    hf.medicaid_or_uninsured_pct = NA,
    hf.nonwhite_rf = NA,
    hf.nonwhite_pct = NA,
    hf.sev4or5 = NA,
    hf.sev4or5_pct = NA,
    hf.ALL_total = NA, 
    hf.ei.ALL_total = NA,
    
    hf.ALL_total_norm = NA,
    hf.medicaid_norm = NA,
    hf.nonwhite_norm = NA,
    hf.sev4or5_norm = NA,
    
    pr.Score=NA, 
    pr.Score10=NA, 
    pr.TotalEDPatients=NA, 
    pr.PedEDPatients=NA,
    pr.guidelines2=3, 
    pr.agreements2=3, 
    pr.guidelines_agreements=3,
    pr.guidelines_agreements2=3
  ))


# res1.1 %>% createTable(show.ratio = TRUE)

# note that categories with missing - ran on reduced set

# stratify by small/rural
res1.1_strat = res1.1 %>% 
  update(fin.small_rural_factor~. - fin.small_rural, include.miss=FALSE)
tables_ = gen_table_results(res1.1_strat, addmd=FALSE, show.all=TRUE)
tables_$table %>% export2csv('2019-07-16_tables_for_draft2/1.1.1_hospitals_overall.csv')
# tables_$missing %>% export2csv('2019-07-16_tables_for_draft2/1.1.1b_hospitals_overall_missing.csv') %>% try()

res1.1_strat = res1.1 %>% 
  update(fin.small_rural_factor~. - fin.small_rural, include.miss=TRUE)
tables_ = gen_table_results(res1.1_strat, addmd=FALSE, show.all=TRUE)
tables_$table %>% export2csv('2019-07-16_tables_for_draft2/1.1.1c_hospitals_overall_missing.csv')


# look at only small/rural
res1.1_strat = res1.1 %>% update(pr.score_cut_70_factor~. - pr.score_cut_70_factor,
                                 subset = fin.small_rural == "TRUE")
tables_ = gen_table_results(res1.1_strat, addmd=FALSE, missing_table = FALSE, show.all=TRUE)
tables_$table %>% export2csv('2019-07-16_tables_for_draft2/1.1.2_hospitals_SR.csv')
remove(res1.1_strat)


# look at only non-(small/rural)
res1.1_strat = res1.1 %>% update(pr.score_cut_70_factor~. - pr.score_cut_70_factor,
                                 subset = fin.small_rural == "FALSE")
tables_ = gen_table_results(res1.1_strat, addmd=FALSE, missing_table=FALSE, show.all=TRUE)
tables_$table %>% export2csv('2019-07-16_tables_for_draft2/1.1.3_hospitals_nonSR.csv')
remove(res1.1_strat)
remove(tables_)




# Individual ----

start.time2 = Sys.time()

message('Univariate & Bivariate: Individual')

individual_vars = c(
  'age', 'age_exact', 'age_cats', 'age_cats2', 'age_cats3', 'age_cats4',
  'sex', 'sex2', 'lang_spoken2', 'lang_spoken3',
  'race_group2', 'race_group2.5', 'race_group3',
  
  'nchs.pat_urban_rural', 'nchs.pat_urban_rural2', 'nchs.pat_urban_rural.int',
  'start_month.factor', 'start_weekend', 'enc_season', 'has.rln',
  'ccs_mental_health', 'ccs_injury',
  'ccs_injury_count', 'ccs_injury_any',
  
  'census.hs_higher', 
  'census.bach_higher', 
  'census.med.income',
  
  'has_ccc',
  'ccc_count',
  'has_ccc_any',
  'has_ccc_any.factor',  
  
  'sev.score', 'sev.score.int',
  'sev.all.max.int',
  'sev.all.max.factor',
  
  'insurance', 'insurance2', 'insurance2.5', 'insurance3',
  
  'pr.score_cut_70_factor',
  'transferred.char'
)
df_ %>% select(individual_vars, 
               fin.small_rural,
               fin.small_rural_factor) %>% skim()
res2.1 = compareGroups(
  ~. - transferred.char -fin.small_rural_factor,  
  data=df_ %>% select(individual_vars, 
                      fin.small_rural,
                      fin.small_rural_factor), 
  method=c(
    age = NA, 
    age_exact = NA, 
    ccs_mental_health=3, has.rln=3, ccs_injury=3,
    ccs_injury_count=3,
    ccs_injury_any=3,
    sev.score.int=NA, 
    sev.all.max.int = NA,
    has_ccc=3,
    ccc_count = NA, 
    has_ccc_any = 3, 
    has_ccc_any.factor = 3,
    census.med.income = NA,
    census.hs_higher = NA,
    census.bach_higher = NA,
    
    fin.small_rural=3,
    fin.small_rural_factor=3,
    transferred.char=3,
    pr.score_cut_70_factor=3)
)

# res2.1 %>% createTable(show.ratio = TRUE)

# note that categories with missing - ran on reduced set

# stratify by small/rural
res2.1_strat = res2.1 %>% 
  update(fin.small_rural_factor~. - fin.small_rural, include.miss=FALSE)
tables_ = gen_table_results(res2.1_strat, addmd=FALSE, show.all=TRUE)
tables_$table %>% export2csv('2019-07-16_tables_for_draft2/2.1.1_individual_overall.csv')
#tables_$missing %>% export2csv('2019-07-16_tables_for_draft2/2.1.1b_individual_overall_missing.csv') %>% try()
remove(res2.1_strat)
remove(tables_)


res2.1_strat = res2.1 %>% 
  update(fin.small_rural_factor~. - fin.small_rural, include.miss=TRUE)
tables_ = gen_table_results(res2.1_strat, addmd=FALSE, show.all=TRUE)
tables_$table %>% export2csv('2019-07-16_tables_for_draft2/2.1.1c_individual_overall_missing.csv')
remove(res2.1_strat)
remove(tables_)

# look at only small/rural
res2.1_strat = res2.1 %>% update(transferred.char~. - transferred.char,
                                 subset = fin.small_rural == "TRUE")
tables_ = gen_table_results(res2.1_strat, addmd=FALSE, missing_table = FALSE, show.all=TRUE)
tables_$table %>% export2csv('2019-07-16_tables_for_draft2/2.1.2_individual_SR.csv')
remove(res2.1_strat)
remove(tables_)

# look at only non-(small/rural)
res2.1_strat = res2.1 %>% update(transferred.char~. - transferred.char,
                                 subset = fin.small_rural == "FALSE")
tables_ = gen_table_results(res2.1_strat, addmd=FALSE, missing_table = FALSE, show.all=TRUE)
tables_$table %>% export2csv('2019-07-16_tables_for_draft2/2.1.3_individual_nonSR.csv')
remove(res2.1_strat)
remove(tables_)


end.time2=Sys.time()
print(paste('Time Finished: ', end.time2))
print(end.time2-start.time2)


message('2nd half of individual data')

start.time2 = Sys.time()

df_ %>% select(hosp_vars_other) %>% skim()

res2.2 = compareGroups(
  ~.-fin.small_rural_factor, 
  data=df_ %>% select(hosp_vars_other, transferred.char),
  method = c(
    nchs.fac_urban_rural.int = NA,
    hf.ped_bed_lic = NA,
    ccs.childrens_hosp = 3,
    hf.has_ped_bed_lic = 3, 
    hf.teach_hosp = 3,
    fin.teaching = 3,
    fin.small_rural = 3, 
    hf.ped_bed_lic = NA,
    hf.trauma_ctr = 3,
    hf.medicaid_rf = NA,
    hf.medicaid_pct = NA,
    hf.medicaid_or_uninsured_rf = NA,
    hf.medicaid_or_uninsured_pct = NA,
    hf.nonwhite_rf = NA,
    hf.nonwhite_pct = NA,
    hf.sev4or5 = NA,
    hf.sev4or5_pct = NA,
    hf.ALL_total = NA, 
    hf.ei.ALL_total = NA,
    
    hf.ALL_total_norm = NA,
    hf.medicaid_norm = NA,
    hf.nonwhite_norm = NA,
    hf.sev4or5_norm = NA,
    
    pr.Score=NA, 
    pr.Score10=NA, 
    pr.TotalEDPatients=NA, 
    pr.PedEDPatients=NA,
    pr.guidelines2=3, 
    pr.agreements2=3, 
    pr.guidelines_agreements=3,
    pr.guidelines_agreements2=3
  ))


res2.2 %>% createTable(show.ratio = TRUE)


# stratify by small/rural
res2.2_strat = res2.2 %>% 
  update(fin.small_rural_factor~. - fin.small_rural, include.miss=FALSE)
tables_ = gen_table_results(res1.1_strat, addmd=FALSE, show.all=TRUE)
tables_$table %>% export2csv('2019-07-16_tables_for_draft2/2.2.1_individual_overall.csv')
#tables_$missing %>% export2csv('2019-07-16_tables_for_draft2/2.2.1b_individual_overall_missing.csv') %>% try()
remove(res2.2_strat)
remove(tables_)

res2.2_strat = res2.2 %>% 
  update(fin.small_rural_factor~. - fin.small_rural, include.miss=TRUE)
tables_ = gen_table_results(res1.1_strat, addmd=FALSE, show.all=TRUE)
tables_$table %>% export2csv('2019-07-16_tables_for_draft2/2.2.1c_individual_overall_missing.csv')
remove(res2.2_strat)
remove(tables_)


# look at only small/rural
res2.2_strat = res2.2 %>% update(transferred.char~. - transferred.char,
                                 subset = fin.small_rural == "TRUE")
tables_ = gen_table_results(res2.2_strat, addmd=FALSE, missing_table = FALSE, show.all=TRUE)
tables_$table %>% export2csv('2019-07-16_tables_for_draft2/2.2.2_individual_SR.csv')
remove(res2.2_strat)
remove(tables_)

# look at only non-(small/rural)
res2.2_strat = res2.2 %>% update(transferred.char~. - transferred.char,
                                 subset = fin.small_rural == "FALSE")
tables_ = gen_table_results(res2.2_strat, addmd=FALSE, missing_table = FALSE, show.all=TRUE)
tables_$table %>% export2csv('2019-07-16_tables_for_draft2/2.2.3_individual_nonSR.csv')
# remove(res2.2_strat)
# remove(tables_)

end.time2=Sys.time()
print(paste('Time Finished: ', end.time2))
print(end.time2-start.time2)


#
# Export data ----
#

subvars = union(hosp_vars_other, individual_vars) %>% unique()

sub_df = df_  %>% subvars
# write.csv(sub_df, "2019-07-16_data.csv")
write.dta(sub_df, "2019-07-16_2012_data.dta")

sub_df = df_ %>% filter(fin.small_rural) %>% subvars
# write.csv(sub_df, "2019-07-16_small_rural_df_2012.csv")
write.dta(sub_df, "2019-07-16_2012_small_rural.dta")

sub_df %>% group_by(oshpd_id) %>% tally() %>% arrange(n)

df_ %>% filter(!fin.small_rural) %>% group_by(oshpd_id) %>% tally() %>% arrange(n) %>% View()

sub_df = df_  %>%  subvars
# write.csv(sub_df, "2019-07-16_small_rural_df_2012.csv")
write.dta(sub_df, "2019-07-16_2012_non_SR.dta")



#
# End timer ----
# 
end.time=Sys.time()
print(paste('Time Finished: ', end.time))
print(end.time-start.time)



