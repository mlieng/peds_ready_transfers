#' db_copy_and_subset
#' 
#' This function copies the 'csv' and imports into the PostgreSQL database
#' Only imports a subset to reduce overall size of database
#' Originally developed in 2018-12-04 Load Database 3
#' 
#' @param file CSV file to be added to the combined database
#' @param oshpd_folder 
#' @param destination Name of table in PostgreSQL database to which the CSV file
#' will be added
#' @param unified_col_order Vector with the names of column. Use to specify the 
#' order that columns should appear, (e.g. important when different years have a
#' different column order). Can also be used to select a subset of columns.
#' Default='*' (e.g. select all in order present)
#' @param subset_command A command for subsetting the data before insertion into 
#' the destination table. Default='agyradm<27'
#' @param diagnostic. Print before/after numbers. Default=TRUE
#' @examples 
#' suppress = dbExecute(con, paste("CREATE TABLE pdd_peds (",pdd_2011_col_types,")"))
#' db_copy_and_subset('Marcin_pdd2011.csv', oshpd_folder,
#'                   pdd_2011_col_types,'pdd_peds',
#'                   subset_command = "agyradm<22")
db_copy_and_subset = function(file, oshpd_folder, 
                              column_designations,
                              destination_table = 'pdd_peds',
                              unified_col_order = "*",
                              subset_command = "agyradm<27",
                              diagnostic=TRUE
){
  # create new temp table using format from header
  suppress_out = dbExecute(con, paste("CREATE TABLE temp_table (",column_designations,")"))
  before.records = dbGetQuery(con, "SELECT COUNT(*) from temp_table")
  
  # read data from year----
  start.time = Sys.time()
  command = paste0("COPY temp_table FROM '",
                   file.path(oshpd_folder,file),
                   "' DELIMITER ',' CSV Header;")
  if(diagnostic) print(command)
  suppress_out = dbExecute(con, command)
  
  end_time = Sys.time()
  if(diagnostic) print(end_time - start.time)
  if(diagnostic) after.records = dbGetQuery(con, "SELECT COUNT(*) from temp_table")
  if(diagnostic) print(cbind(before = before.records[1,1] %>% as.integer(), 
                             after=after.records[1,1] %>% as.integer()))
  
  # copy peds data from year----
  start.time = Sys.time()
  before.peds.number = dbGetQuery(
    con, paste("SELECT COUNT(*) from", destination_table))
  command = paste("INSERT INTO",destination_table,
                  "SELECT",unified_col_order,
                  "FROM temp_table WHERE", subset_command)
  if (diagnostic) print (command)
  dbExecute(con, command)
  end_time = Sys.time()
  if(diagnostic) print(end_time - start.time)
  after.peds.number = dbGetQuery(
    con, paste("SELECT COUNT(*) from", destination_table))
  if(diagnostic) print(cbind(before = before.peds.number[1,1] %>% as.integer(), 
                             after=after.peds.number[1,1] %>% as.integer()))
  
  # reset/clear temp table----
  suppress_out = dbExecute(con, "DROP TABLE temp_table")
}


#' db_recode
#' 
#' Recode a variable in PostgreSQL, similar to dplyr::recode
#' Developed in Normalize #2 (2018-12-11 RenameVars_RecodeVars2)
db_recode = function(table, column, code_map){
  table_col = paste0(table,'.',column)
  dbWriteTable(con, 'temp_table', code_map)
  command = paste('UPDATE',table, 'SET', column, 
                  '= temp_table.to FROM temp_table WHERE', table_col, 
                  '= temp_table.from' )
  dbExecute(con, command)
  out = dbExecute(con, 'DROP table temp_table') # suppresses output
  return(out)
}

#' db_compare_two_cols
#' 
#' Can be used to compare recoded variables
#' Developed in Normalize #2 (2018-12-11 RenameVars_RecodeVars2)
db_compare_two_cols = function(table, col1, col2){
  res = dbGetQuery(
    con, paste("SELECT",col1,", count(",col1,") FROM ",table," GROUP BY", col1))
  res2 = dbGetQuery(
    con, paste("SELECT",col2,", count(",col2,") FROM ",table," GROUP BY", col2))
  cbind(arrange(res,count),arrange(res2, count)) %>% print() 
}

#' list_to_df
#' 
#' Simple wrapper function to convert a list to a data.frame with the columns
#' 'From' and 'To'. Use with db_recode.
#' 
#' @example 
#' \dontrun{
#'   suppress_out = db_recode('subset_pdd_peds', 'admtday',
#' code_map=list(
#'   `1` = 'Sun',
#'   `2` = 'Mon', 
#'   `3` = 'Tue', 
#'   `4` = 'Wed', 
#'   `5` = 'Thu',
#'   `6` = 'Fri',
#'   `7` = 'Sat'
#' ) %>% list_to_df()
#' }
#' 
#' Developed in Normalize #2 (2018-12-11 RenameVars_RecodeVars2)
list_to_df = function(list_){
  plyr::ldply(list_,data.frame) %>% `colnames<-`(c("from", "to"))
}

#' recode_add_PDD_subset
#' 
#' Recodes certains columns in the PD dataset
#' Inserts in prescribed order into the combined table
#' Adds columns that are present in the ED data set
#' Developed 2018/12/19 RenameVars Recode Vars #3
#' @examples 
#' print('--2016--')
#' # create a subset of data
#' dbExecute(con, "SELECT * INTO subset_pdd_peds FROM pdd_peds WHERE dsch_yr=2016") 
#' # insert into combined data table
#' recode_add_PDD_subset()
recode_add_PDD_subset <- function(){
  start.time = Sys.time()
  # add columns
  suppress_out=dbExecute(con, "
                         ALTER TABLE subset_pdd_peds 
                         ALTER COLUMN sex TYPE TEXT,
                         ALTER COLUMN admtday TYPE TEXT,
                         ALTER COLUMN pay_cat TYPE TEXT, 
                         ADD column database varchar(3),
                         ADD column insurance TEXT,
                         ADD column ed_disp INT,
                         ADD column ed_pr_prin TEXT,
                         ADD column ed_opr1 TEXT,
                         ADD column ed_opr2 TEXT,
                         ADD column ed_opr3 TEXT,
                         ADD column ed_opr4 TEXT,
                         ADD column row_n INT,
                         ADD column db_row_n INT,
                         ADD column id INT
                         ")
  # convert string to date and assign other
  suppress_out=dbExecute(con, "
                         UPDATE subset_pdd_peds 
                         SET admtdate = to_date(admtdate, 'MM/DD/YYYY'),
                         dschdate = to_date(dschdate,'MM/DD/YYYY'),
                         bthdate = to_date(bthdate, 'MM/DD/YYYY'),
                         database = 'pdd',
                         insurance = pay_cat")
  # recode
  suppress_out = db_recode('subset_pdd_peds', 'sex',
                           code_map=data.frame(
                             from=c('.','1','2','3','4'),
                             to=c('Invalid','Male', 'Female', 'Other','Invalid')
                           ))
  suppress_out = db_recode('subset_pdd_peds', 'insurance',
                           code_map=list(
                             `1` = 'Public-Medicare-MediCal',
                             `2` = 'Public-Medicare-MediCal', 
                             `3` = 'Private', 
                             `4` = 'Other', 
                             `5` = 'Public-Other',
                             `6` = 'Public-Other',
                             `7` = 'Other',
                             `8` = 'Self-Pay',
                             `9` = 'Other',
                             `0` = 'Unknown-Invalid'
                           ) %>% list_to_df()
  )
  suppress_out = db_recode('subset_pdd_peds', 'admtday',
                           code_map=list(
                             `1` = 'Sun',
                             `2` = 'Mon', 
                             `3` = 'Tue', 
                             `4` = 'Wed', 
                             `5` = 'Thu',
                             `6` = 'Fri',
                             `7` = 'Sat'
                           ) %>% list_to_df()
  )
  # insertion
  suppress_out = dbExecute(con, "
    INSERT INTO combined_peds 
   SELECT 
   nextval('record_id_seq'), 
   ROW_NUMBER() OVER (ORDER BY admtdate) AS db_row_n, database, 
   rln, sex, bthdate AS birthdate, 
   agyradm AS age, agyrdsch AS end_age, agdyadm AS age_days,
   race_grp AS race_group, pls_abbr AS lang_spoken,
   patzip AS pat_zip, patcnty AS pat_county,
   pay_cat AS payer, insurance, 
   oshpd_id, hplzip AS fac_zip, hplcnty as fac_county,
   typcare AS ip_fac_type,
   admtdate AS start_date, dschdate AS end_date, admtday AS start_day_of_week,
   admtyr AS start_year, dsch_yr AS end_year,
   qtr_adm AS start_quarter, qtr_dsch AS end_quarter,
   admtmth AS start_month,
   los, los_adj, charge AS ip_charge, 
   srcsite AS ip_source_site, srclicns AS ip_source_license, 
   srcroute AS ip_source_route, admtype AS ip_admission_type,
   ed_disp, disp AS ip_disp,
   diag_p AS dx_prin, 
   odiag1 AS odx1, odiag2 AS odx2, odiag3 AS odx3, 
   odiag4 AS odx4, odiag5 AS odx5,  odiag6 AS odx6,  
   odiag7 AS odx7, odiag8 AS odx8,  odiag9 AS odx9,  odiag10 AS odx10, 
   odiag11 AS odx11, odiag12 AS odx12, odiag13 AS odx13, 
   odiag14 AS odx14, odiag15 AS odx15, odiag16 AS odx16,
   odiag17 AS odx17, odiag18 AS odx18, odiag19 AS odx19, odiag20 AS odx20, 
   odiag21 AS odx21, odiag22 AS odx22, odiag23 AS odx23, odiag24 AS odx24,
   poa_p AS dx_poa_prin,
   ccs_diagp AS ccs_dx_prin, ccs_odiag1 AS ccs_odx1, ccs_odiag2 AS ccs_odx2,
   ccs_odiag3 AS ccs_odx3, ccs_odiag4 AS ccs_odx4, ccs_odiag5 AS ccs_odx5,
   ccs_odiag6 AS ccs_odx6, ccs_odiag7 AS ccs_odx7, ccs_odiag8 AS ccs_odx8,
   ccs_odiag9 AS ccs_odx9, ccs_odiag10 AS ccs_odx10,
   ecode_p AS ec_prin, ecode1 AS ec1, ecode2 AS ec2, ecode3 AS ec3, ecode4 AS ec4,
   ed_pr_prin, ed_opr1, ed_opr2, ed_opr3, ed_opr4,
   epoa_p AS ec_poa_prin, epoa1 AS ec_poa1, epoa2 AS ec_poa2, epoa3 AS ec_poa3, 
   epoa4 AS ec_poa4,
   proc_p AS ip_pr_prin, oproc1 AS ip_opr1, oproc2 AS ip_opr2, 
   oproc3 AS ip_opr3, oproc4 AS ip_opr4, oproc5 AS ip_opr5,
   proc_pdy AS ip_day_pr_prin, procdy1 AS ip_day_opr1, procdy2 AS ip_day_opr2,
   procdy3 AS ip_day_opr3, procdy4 AS ip_day_opr4, procdy5 AS ip_day_opr5,
   mdc AS ip_mdc, msdrg AS ip_msdrg, grouper AS grouper_v, 
   sev_code AS ip_sev_code, cat_code AS ip_cat_code
                           
       FROM subset_pdd_peds")
  
  end.time=Sys.time()
  print(paste('Time Finished: ', end.time))
  print(end.time-start.time)
}


#' recode_add_EDD_subset
#' 
#' Recodes certains columns in the ED dataset
#' Inserts in prescribed order into the combined table
#' Adds columns that are present in the PD data set
#' Developed 2018/12/19 RenameVars Recode Vars #3
recode_add_EDD_subset <- function(){
  start.time = Sys.time()
  # add columns
  
  suppress_out=dbExecute(con, "
                         ALTER TABLE subset_edd_peds
                         ALTER COLUMN patzip TYPE TEXT,
                         ALTER column serv_d TYPE TEXT, 
                         ADD column database varchar(3),
                         ADD column insurance TEXT,
                         ADD column los INT,
                         ADD column los_adj INT,
                         ADD column ip_source_site INT,
                         ADD column ip_source_license INT,
                         ADD column ip_source_route INT,
                         ADD column ip_admission_type INT,
                         ADD column ip_charge INT,
                         ADD column ip_disp INT,
                         ADD column ip_fac_type INT,
                         ADD column dx_poa_prin TEXT,
                         ADD column ec_poa_prin TEXT,
                         ADD column ec_poa1 TEXT,
                         ADD column ec_poa2 TEXT,
                         ADD column ec_poa3 TEXT,
                         ADD column ec_poa4 TEXT,
                         ADD column ip_pr_prin TEXT,
                         ADD column ip_opr1 TEXT,
                         ADD column ip_opr2 TEXT,
                         ADD column ip_opr3 TEXT,
                         ADD column ip_opr4 TEXT,
                         ADD column ip_opr5 TEXT,
                         ADD column ip_day_pr_prin INT,
                         ADD column ip_day_opr1 INT,
                         ADD column ip_day_opr2 INT,
                         ADD column ip_day_opr3 INT,
                         ADD column ip_day_opr4 INT,
                         ADD column ip_day_opr5 INT,
                         ADD column ip_mdc INT,
                         ADD column ip_msdrg INT,
                         ADD column grouper_v float,
                         ADD column ip_sev_code INT,
                         ADD column ip_cat_code TEXT,
                         ADD column row_n INT,
                         ADD column db_row_n INT
                         ")
  # convert string to date and assign other
  suppress_out=dbExecute(con, "
                         UPDATE subset_edd_peds 
                         SET serv_dt = to_date(serv_dt, 'MM/DD/YYYY'),
                         brthdate = to_date(brthdate, 'MM/DD/YYYY'),
                         database = 'edd',
                         insurance = payer,
                         los=0")
  # recode
  suppress_out = db_recode('subset_edd_peds', 'sex', 
                           code_map=data.frame(
                             from = c('M', 'F', 'U','I') , 
                             to = c('Male', 'Female','Unknown','Invalid')))
  suppress_out = db_recode('subset_edd_peds', 'insurance',
                           code_map=list(
                             `09` = 'Self-Pay',
                             `11` = 'Other', 
                             `12` = 'Private', 
                             `13` = 'Private', 
                             `14` = 'Private',
                             `16` = 'Private',
                             AM = 'Other',
                             BL = 'Private',
                             CH = 'Public-Other',
                             CI = 'Private',
                             DS = 'Other',
                             HM = 'Private',
                             MA = 'Public-Medicare-MediCal',
                             MB = 'Public-Medicare-MediCal',
                             MC = 'Public-Medicare-MediCal',
                             OF = 'Public-Other',
                             TV = 'Public-Other',
                             VA = 'Public-Other',
                             WC = 'Other',
                             `00` = 'Other',
                             `99` = 'Unknown-Invalid') %>% list_to_df()
  )
  suppress_out = db_recode('subset_edd_peds', 'serv_d',
                           code_map=list(
                             `1` = 'Sun',
                             `2` = 'Mon', 
                             `3` = 'Tue', 
                             `4` = 'Wed', 
                             `5` = 'Thu',
                             `6` = 'Fri',
                             `7` = 'Sat'
                           ) %>% list_to_df()
  )
  suppress_out = dbExecute(con, "
    INSERT INTO combined_peds 
     SELECT 
     nextval('record_id_seq'), 
     ROW_NUMBER() OVER (ORDER BY serv_dt) AS db_row_n, database, 
     rln, sex, brthdate as birthdate, 
     agyrserv age, agyrserv end_age, agdyserv AS age_days,
     race_grp AS race_group, pls_abr AS lang_spoken, 
     patzip AS pat_zip, patco AS pat_county, 
     payer, insurance,
     fac_id AS oshpd_id, faczip AS fac_zip, fac_co AS fac_county, ip_fac_type,
     serv_dt AS start_date, serv_dt AS end_date, serv_d AS start_day_of_week,
     serv_y AS start_year, serv_y AS end_year,
     serv_q AS start_quarter, serv_q AS end_quarter,
     serv_m AS start_month,
     los, los_adj, ip_charge, ip_source_site, ip_source_license, ip_source_route,
     ip_admission_type,
     
     dispn AS ed_disp, ip_disp,
     dx_prin, odx1, odx2, odx3, odx4, odx5, odx6, odx7, odx8, odx9, odx10,
     odx11, odx12, odx13, odx14, odx15, odx16, odx17, odx18, odx19, odx20,
     odx21, odx22, odx23, odx24,
     dx_poa_prin,
     ccs_dx_prin, ccs_odx1, ccs_odx2, ccs_odx3, ccs_odx4, ccs_odx5,
     ccs_odx6, ccs_odx7, ccs_odx8, ccs_odx9, ccs_odx10,
     ec_prin, ec1, ec2, ec3, ec4,
     ec_poa_prin, ec_poa1, ec_poa2, ec_poa3, ec_poa4,
     pr_prin AS ed_pr_prin, opr1 AS ed_opr1, opr2 AS ed_opr2,
     opr3 AS ed_opr3, opr4 AS ed_opr4,
     ip_pr_prin, ip_opr1, ip_opr2, ip_opr3, ip_opr4, ip_opr5,
     ip_day_pr_prin, ip_day_opr1, ip_day_opr2, ip_day_opr3, ip_day_opr4, ip_day_opr5,
     ip_mdc, ip_msdrg, grouper_v, ip_sev_code, ip_cat_code
                           
         FROM subset_edd_peds")
  
  end.time=Sys.time()
  print(paste('Time Finished: ', end.time))
  print(end.time-start.time)
}