library(dplyr)
library(stringr)

# Load files

ccs_multi_dx_tool9 = read.csv('ccs/ccs_multi_dx_tool_2015.csv', stringsAsFactors=F)
ccs_multi_dx_tool10 = read.csv('ccs/ccs_dx_icd10cm_2018_1.csv', stringsAsFactors=F)

# Pre-processing

ccs_labels = c(
  'Infectious and parasitic diseases',
  'Neoplasms',
  'Endocrine; nutritional; and metabolic diseases and immunity disorders',
  'Diseases of the blood and blood-forming organs',
  'Mental Illness',
  'Diseases of the nervous system and sense organs',
  'Diseases of the circulatory system',
  'Diseases of the respiratory system',
  'Diseases of the digestive system',
  'Diseases of the genitourinary system',
  'Complications of pregnancy; childbirth; and the puerperium',
  'Diseases of the skin and subcutaneous tissue',
  'Diseases of the musculoskeletal system and connective tissue',
  'Congenital anomalies',
  'Certain conditions originating in the perinatal period',
  'Injury and poisoning',
  'Symptoms; signs; and ill-defined conditions and factors influencing health status',
  'Residual codes; unclassified; all E codes [259. and 260.]'
)


ccs9 = ccs_multi_dx_tool9 %>%
  mutate(dx_code = X.ICD.9.CM.CODE. %>% str_remove_all("\'") %>% str_trim(),
         ccs = X.CCS.LVL.1. %>% str_remove_all("\'") %>%
           factor(levels=1:18, labels=ccs_labels)) %>%
  select(dx_code, ccs)

ccs10 = ccs_multi_dx_tool10 %>%
  # clean up
  mutate(dx_code =  X.ICD.10.CM.CODE. %>% str_remove_all("\'") %>% str_trim(),
         ccs = X.MULTI.CCS.LVL.1. %>% str_remove_all("\'") %>%
           factor(levels=1:18, labels=ccs_labels)) %>%
  select(dx_code, ccs)

remove(ccs_labels)


# Functions

icd9_to_ccs = function(wide_data){
  id_col = colnames(wide_data)[1]
  if(nrow(wide_data)!=length(unique(wide_data[[id_col]]))) stop("First column should be a unique ID column")

  long_format = wide_data %>% gather('dx_col', 'dx_code', -one_of(id_col), factor_key=TRUE)
  wide_format = long_format %>%
    # join level 1 - CCS categories
    left_join(ccs9, by='dx_code') %>%
    select(-dx_code) %>%
    spread(dx_col, ccs)
  return(wide_format)
}


icd10_to_ccs = function(wide_data){
  id_col = colnames(wide_data)[1]
  if(nrow(wide_data)!=length(unique(wide_data[[id_col]]))) stop("First column should be a unique ID column")

  long_format = wide_data %>% gather('dx_col', 'dx_code', -one_of(id_col), factor_key=TRUE)
  wide_format = long_format %>%
    # join level 1 - CCS categories
    left_join(ccs10, by='dx_code') %>%
    select(-dx_code) %>%
    spread(dx_col, ccs)
  return(wide_format)
}


add_injury_cols = function(wide_data){
  id_col = colnames(wide_data)[1]
  if(nrow(wide_data)!=length(unique(wide_data[[id_col]]))) stop("First column should be a unique ID column")

  df_has_injury = wide_data%>% select_at(vars(-id_col)) %>%
    mutate_all(~(.=='Injury and poisoning'))

  wide_data = wide_data %>% mutate(
    ccs_injury_count = apply(df_has_injury, 1, function(x) sum(x, na.rm=TRUE)),
    ccs_injury_any = ccs_injury_count >=1,
    ccs_injury_prin = dx_prin == 'Injury and poisoning'
  )


  return(wide_data)
}



# Example

# test_icd9 = data_ %>% filter(start_date < '2015-10-01') %>%
#   select(sql_id, 'dx_prin', starts_with('odx')) %>% icd9_to_ccs()
#
# test_icd10 = data_ %>% filter(start_date >= '2015-10-01') %>%
#   select(sql_id, 'dx_prin', starts_with('odx')) %>% icd10_to_ccs()
#
# combined_ccs = bind_rows(test_icd9, test_icd10)
# combined_ccs = add_injury_cols(combined_ccs)
#
# subset_combined_ccs = combined_ccs %>%
#   select(sql_id, dx_prin, ccs_injury_count, ccs_injury_any, ccs_injury_prin) %>%
#   rename(ccs_dx_prin = dx_prin)
#
# data_ = data_ %>% left_join(subset_combined_ccs, by='sql_id')


# Original Code

#
# __Clinical Classifications Software (18 categories) ----
#

# ccs = ccs_multi_dx_tool %>%
#   # clean up
#   mutate(dx_prin = X.ICD.9.CM.CODE. %>% str_remove_all("\'") %>% str_trim(),
#          ccs_group = as.integer(X.CCS.LVL.1.),
#          # combine 'Mental illness' with '
#          ccs_label = X.CCS.LVL.1.LABEL. %>% str_replace('Mental illness', 'Mental Illness')) %>%
#   select(dx_prin, ccs_group, ccs_label)
#
# # translate principle diagnosis
# odx_max = 24
#
# data_ = left_join(data_, ccs, by='dx_prin')
#
# # translate other diagnoses
# orig_cols = c('dx_prin', 'ccs_group', 'ccs_label')
# for(i in seq(1:odx_max)){
#   new_cols = c(paste0('odx',i), paste0('ccs_group.',i), paste0('ccs_label.',i))
#   data_ = ccs %>%
#     # rename columns to the appropriate number (e.g. dx_prin -> odx1, ccs_group --> ccs_group.1)
#     select(orig_cols) %>% setNames(new_cols) %>%
#     left_join(data_, ., by=paste0('odx',i))
# }
#
# # count number of injury codes
# df_has_injury = data_%>% select(starts_with('ccs_label')) %>%
#   mutate_all(~(.=='Injury and poisoning'))
# data_$ccs_injury_count = apply(df_has_injury, 1, function(x) sum(x, na.rm=TRUE))
# remove(df_has_injury)
#
# # convert counting
# data_ = data_ %>% mutate(
#   ccs_injury_any = ccs_injury_count >= 1, #replace_na(FALSE), unnecessary
#   ccs_mental_health = ccs_label =='Mental Illness',
#   ccs_injury = ccs_label == 'Injury and poisoning'
# )
