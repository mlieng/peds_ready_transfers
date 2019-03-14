#' this is a test script
#' 
#' 
password = 'ucdavis'
peds_ready_folder = file.path(getwd(),'local_data','peds_ready')


#' Set-Up
library(RPostgres) # database
library(tidyverse) # includes
# dplyr(data munging), ggplot2 (plotting), tidyr (tidy data)
# library(dplyr) # data munging

pilot = read.csv('local_data/peds_ready/2012_CA_PediatricReadinessPilotData.csv')
combined_peds_cols = 

#' Test DBI, based on load database #3
con = dbConnect(RPostgres::Postgres(),
                dbname='postgres_oshpd', 
                host="localhost", port=5432,
                user='postgres',
                password=password)

dbListTables(con)
dbWriteTable(con, 'pilot', pilot, row.names=FALSE, overwrite=TRUE)
dbGetQuery(con, "COPY pilot FROM 
           'local_data/peds_ready/2012_CA_PediatricReadinessPilotData.csv'
           DELIMITER ',' CSV HEADER;")

dbGetQuery(con, "
COPY pilot FROM 
'/Users/mlieng1/Documents/pat_data_and_analysis/local_data/peds_ready/2012_CA_PediatricReadinessPilotData.csv' DELIMITER ',' CSV HEADER;")


res = dbSendQuery(con, "create table Foo1 (f1 int)")
res = dbGetQuery(con, "create table Foo2 (f1 int)")


# Test DBI 2

con2 = dbConnect(RPostgres::Postgres(),
                dbname='oshpd_postgres', 
                host="localhost", port=5432,
                user='postgres',
                password=password)

dbListTables(con2)
res = dbGetQuery(con, "create table Foo1 (f1 int)")
dbListTables(con2)


dbWriteTable(con2, 'pilot', pilot, row.names=FALSE, overwrite=TRUE)

dbGetQuery(con2, "
COPY pilot FROM 
           '/Users/mlieng1/Documents/pat_data_and_analysis/local_data/peds_ready/2012_CA_PediatricReadinessPilotData.csv' DELIMITER ',' CSV HEADER;")


command = paste("COPY peds_ready_test FROM '",
                file.path(peds_ready_folder,'2012_CA_PediatricReadinessPilotData.csv'),
                "'DELIMITER ',' CSV Header;")

dbGetQuery(con2, command)


# doesn't seem like I can upload the pediatric readiness data, so may be easier to just do full data


file='Marcin_pdd2011.csv'

command = paste0("\\copy temp_table FROM '",
                 file.path(oshpd_folder,file),
                 "'DELIMITER ',' CSV Header;")

command = paste0("copy temp_table FROM '",
                 file.path('local_data/oshpd_full_data',file),
                 "' DELIMITER ',' CSV Header;")
if(diagnostic) print(command)
dbExecute(con, command)




