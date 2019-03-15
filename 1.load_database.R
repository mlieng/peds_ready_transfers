#
# Set-up ----
#

# Import custom functions

source('1.1_supporting_functions.R')

# Import libraries
library(RPostgres) # database
library(tidyverse) # includes
# dplyr(data munging), ggplot2 (plotting), tidyr (tidy data)
# library(dplyr) # data munging

# Import files with column designations

oshpd_folder = file.path(getwd(),'local_data','oshpd_full_data')

pdd_col_order = read.csv(
  file.path(oshpd_folder,'2018-12-07_pdd_column_order_and_types.csv'),
  header = TRUE, stringsAsFactors = FALSE)
edd_col_order = read.csv(
  file.path(oshpd_folder,'2018-12-07_edd_column_order_and_types.csv'),
  header = TRUE, stringsAsFactors = FALSE)
combined_peds_cols = read.csv(
  file.path(oshpd_folder,'2018-12-19_combined_table_designations.csv'), 
  header=TRUE, stringsAsFactors = FALSE)

# hack - need to rename since '2011' gets read as 'i..2011'
colnames(pdd_col_order) = c("X2011", "X2011_types", "X2016", "X2016_types")
colnames(edd_col_order) = c(
  "X2011", "X2011_types", "X2015", "X2015_types","X2016", "X2016_types"
)
colnames(combined_peds_cols) = c(
  "name", "designation", "sql_command"
)

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
# Set PD table----
#

pdd_2011_col_types = paste(pdd_col_order$X2011_types, collapse=', ')
pdd_2011_order = paste(pdd_col_order$X2011, collapse=', ')
pdd_2016_col_types = paste(pdd_col_order$X2016_types, collapse=', ')

#
# Copy PDD databases----
#

start.time = Sys.time()
suppress = dbExecute(con, paste("CREATE TABLE pdd_peds (",pdd_2011_col_types,")"))

db_copy_and_subset('Marcin_pdd2011.csv', oshpd_folder,
                   pdd_2011_col_types,'pdd_peds',
                   subset_command = "agyradm<22")
db_copy_and_subset('Marcin_pdd2012.csv', oshpd_folder,
                   pdd_2011_col_types,'pdd_peds',
                   subset_command = "agyradm<22")
db_copy_and_subset('Marcin_pdd2013.csv', oshpd_folder,
                   pdd_2011_col_types,'pdd_peds',
                   subset_command = "agyradm<22")
db_copy_and_subset('Marcin_pdd2014.csv', oshpd_folder,
                   pdd_2011_col_types,'pdd_peds',
                   subset_command = "agyradm<22")
db_copy_and_subset('Marcin_pdd2015.csv', oshpd_folder,
                   pdd_2016_col_types,'pdd_peds', pdd_2011_order,
                   subset_command = "agyradm<22")
db_copy_and_subset('Marcin_pdd2016.csv', oshpd_folder,
                   pdd_2016_col_types,'pdd_peds', pdd_2011_order,
                   subset_command = "agyradm<22")
end.time=Sys.time()
print(paste('Time Finished: ', end.time))
print(end.time-start.time)

tbl(con, 'pdd_peds') %>% select(dsch_yr, agyradm) %>% collect() %>% table()

#
# Set up ED table----
#

edd_2011_col_types = paste(edd_col_order$X2011_types, collapse=', ')
edd_2011_order = paste(edd_col_order$X2011, collapse=', ')
edd_2015_col_types = paste(edd_col_order$X2015_types, collapse=', ')
edd_2016_col_types = paste(edd_col_order$X2016_types, collapse=', ')

suppress = dbExecute(con, paste("CREATE TABLE edd_peds (",edd_2011_col_types,")"))

#
# Copy EDD databases----
#

start.time=Sys.time()
db_copy_and_subset('Marcin_ed2011.csv', oshpd_folder,
                   edd_2011_col_types, 'edd_peds',
                   subset_command='agyrserv<22')
db_copy_and_subset('Marcin_ed2012.csv', oshpd_folder,
                   edd_2011_col_types, 'edd_peds' ,
                   subset_command='agyrserv<22')
db_copy_and_subset('Marcin_ed2013.csv',  oshpd_folder,
                   edd_2011_col_types, 'edd_peds',
                   subset_command='agyrserv<22')
db_copy_and_subset('Marcin_ed2014.csv', oshpd_folder,
                   edd_2011_col_types, 'edd_peds',
                   subset_command='agyrserv<22')
db_copy_and_subset('Marcin_ed2015.csv', oshpd_folder,
                   edd_2015_col_types, 'edd_peds', edd_2011_order,
                   subset_command='agyrserv<22')
db_copy_and_subset('Marcin_ed2016.csv', oshpd_folder,
                   edd_2016_col_types, 'edd_peds', edd_2011_order,
                   subset_command='agyrserv<22')
end.time=Sys.time()
print(paste('Time Finished: ', end.time))
print(end.time-start.time)
tbl(con, 'edd_peds') %>% select(serv_y, agyrserv) %>% collect() %>% table()


#
# Create combined table -----
#

# suppress_out = dbExecute(con, "DROP TABLE combined_peds")
command = paste0(
  "CREATE TABLE combined_peds(",
  paste(combined_peds_cols$sql_command, collapse=", "),
  ")"
)
print(command)
suppress_out = dbExecute(con, command)

# creates a new ID column that automatically generates(in first column)
suppress_out = dbExecute(con, " CREATE TABLE combined_peds2 
                         (id serial primary key, like combined_peds INCLUDING ALL)")
suppress_out = dbExecute(con, "DROP TABLE combined_peds")
suppress_out = dbExecute(con, 
                         "ALTER TABLE combined_peds2 RENAME TO combined_peds")

#suppress_out = dbExecute(con, "DROP SEQUENCE record_id_seq")
suppress_out = dbExecute(con, "CREATE SEQUENCE record_id_seq")

#
# Add PD datasets to combined table ----
#

start.time = Sys.time()
# suppress_out = dbExecute(con, "DROP TABLE subset_pdd_peds")

print('--2011--')
dbExecute(con, "SELECT * INTO subset_pdd_peds FROM pdd_peds WHERE dsch_yr=2011") 
recode_add_PDD_subset()
suppress_out = dbExecute(con, "DROP TABLE subset_pdd_peds")

print('--2012--')
dbExecute(con, "SELECT * INTO subset_pdd_peds FROM pdd_peds WHERE dsch_yr=2012") 
recode_add_PDD_subset()
suppress_out = dbExecute(con, "DROP TABLE subset_pdd_peds")

print('--2013--')
dbExecute(con, "SELECT * INTO subset_pdd_peds FROM pdd_peds WHERE dsch_yr=2013") 
recode_add_PDD_subset()
suppress_out = dbExecute(con, "DROP TABLE subset_pdd_peds")

print('--2014--')
dbExecute(con, "SELECT * INTO subset_pdd_peds FROM pdd_peds WHERE dsch_yr=2014") 
recode_add_PDD_subset()
suppress_out = dbExecute(con, "DROP TABLE subset_pdd_peds")

print('--2015--')
dbExecute(con, "SELECT * INTO subset_pdd_peds FROM pdd_peds WHERE dsch_yr=2015") 
recode_add_PDD_subset()
suppress_out = dbExecute(con, "DROP TABLE subset_pdd_peds")

print('--2016--')
dbExecute(con, "SELECT * INTO subset_pdd_peds FROM pdd_peds WHERE dsch_yr=2016") 
recode_add_PDD_subset()
# suppress_out = dbExecute(con, "DROP TABLE subset_pdd_peds")

print('--------------------------------')
print('All done copying years 2011-2016')
end.time=Sys.time()
print(paste('Time Finished: ', end.time))
print(end.time-start.time)


#
# Add ED datasets to combined table ----
#

start.time = Sys.time()
suppress_out = dbExecute(con, "DROP TABLE subset_edd_peds")

print('--2011--')
dbExecute(con, "SELECT * INTO subset_edd_peds FROM edd_peds WHERE serv_y=2011") 
recode_add_EDD_subset()
suppress_out = dbExecute(con, "DROP TABLE subset_edd_peds")

print('--2012--')
dbExecute(con, "SELECT * INTO subset_edd_peds FROM edd_peds WHERE serv_y=2012") 
recode_add_EDD_subset()
suppress_out = dbExecute(con, "DROP TABLE subset_edd_peds")

print('--2013--')
dbExecute(con, "SELECT * INTO subset_edd_peds FROM edd_peds WHERE serv_y=2013") 
recode_add_EDD_subset()
suppress_out = dbExecute(con, "DROP TABLE subset_edd_peds")

print('--2014--')
dbExecute(con, "SELECT * INTO subset_edd_peds FROM edd_peds WHERE serv_y=2014") 
recode_add_EDD_subset()
suppress_out = dbExecute(con, "DROP TABLE subset_edd_peds")

print('--2015--')
dbExecute(con, "SELECT * INTO subset_edd_peds FROM edd_peds WHERE serv_y=2015") 
recode_add_EDD_subset()
suppress_out = dbExecute(con, "DROP TABLE subset_edd_peds")

print('--2016--')
dbExecute(con, "SELECT * INTO subset_edd_peds FROM edd_peds WHERE serv_y=2016") 
recode_add_EDD_subset()
# suppress_out = dbExecute(con, "DROP TABLE subset_edd_peds")

print('--------------------------------')
print('All done copying years 2011-2016')
end.time=Sys.time()
print(paste('Time Finished: ', end.time))
print(end.time-start.time)


#
# Validate Combination ----
#

# view header
dbGetQuery(con, "SELECT * FROM combined_peds LIMIT 10")

# count by database 
res = dbGetQuery(
  con, "SELECT database, count(database) FROM combined_peds GROUP BY database")
res %>% print()
#res %>% arrange(database) 

# count by database and year
res2 = dbGetQuery(
  con, "
  SELECT database, end_year, count(database) 
  FROM combined_peds GROUP BY database, end_year")
res2 %>% print()


# count by database and year
res3 = dbGetQuery(
  con, "
  SELECT database, start_year, count(database) 
  FROM combined_peds GROUP BY database, start_year")
res3 %>% print()