# Parameters----

password = 'ucdavis'

pdd_col_order = read.csv(
  'local_data/oshpd_full_data/2018-12-07_pdd_column_order_and_types.csv')
edd_col_order = read.csv(
  'local_data/oshpd_full_data/2018-12-07_edd_column_order_and_types.csv')
oshpd_folder = file.path(getwd(),'local_data','oshpd_full_data')


#
# Set-up----
#

library(RPostgres) # database
library(tidyverse) # includes
# dplyr(data munging), ggplot2 (plotting), tidyr (tidy data)
# library(dplyr) # data munging

#
# Custom functions----
#

db_copy_and_subset = function(file, oshpd_folder, 
                              column_designations,
                              destination_table = 'pdd_peds',
                              unified_col_order = "*",
                              subset_command = "agyradm<27",
                              diagnostic=TRUE
){
  # create new temp table using format from header
  dbExecute(con, paste("CREATE TABLE temp_table (",column_designations,")"))
  before.records = dbGetQuery(con, "SELECT COUNT(*) from temp_table")
  
  # read data from year----
  start.time = Sys.time()
  command = paste0("COPY temp_table FROM '",
                   file.path(oshpd_folder,file),
                   "'DELIMITER ',' CSV Header;")
  if(diagnostic) print(command)
  dbExecute(con, command)
  
  end_time = Sys.time()
  if(diagnostic) print(end_time - start.time)
  if(diagnostic) after.records = dbGetQuery(con, "SELECT COUNT(*) from temp_table")
  if(diagnostic) print(c(before.records, after.records))
  if(diagnostic) print(list(before=before.records, after=after.records))
  
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
  if(diagnostic) print(list(before=before.peds.number, after=after.peds.number))
  
  # reset/clear temp table----
  dbExecute(con, "DROP TABLE temp_table")
}


#
# Connect to database----
#

con = dbConnect(RPostgres::Postgres(),
                 dbname='oshpd_postgres', 
                 host="localhost", port=5433,
                 user='postgres',
                 password=password)
dbListTables(con)


#
# Set PD table----
#

pdd_2011_col_types = paste(pdd_col_order$X2011_types, collapse=", ")
pdd_2011_order = paste(pdd_col_order$X2011, collapse=", ")
pdd_2016_col_types = paste(pdd_col_order$X2016_types, collapse=", ")

suppress = dbExecute(con, paste("CREATE TABLE pdd_peds (",pdd_2011_col_types,")"))

#
# Copy PDD databases----
#

dbExecute(con, "DROP TABLE pdd_peds")
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

edd_2011_col_types = paste(edd_col_order$X2011_types, collapse=", ")
edd_2011_order = paste(edd_col_order$X2011, collapse=", ")
edd_2015_col_types = paste(edd_col_order$X2015_types, collapse=", ")
edd_2016_col_types = paste(edd_col_order$X2016_types, collapse=", ")

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
tbl(con, 'edd_peds') %>% select(serv_y, agyrserv) %>% collect() %>% table()

