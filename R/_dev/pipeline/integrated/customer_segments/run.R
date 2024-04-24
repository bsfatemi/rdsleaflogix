##
## This script should be run weekly
##
box::use(
  data.table[...],
  logr[log_close, log_open, put, sep],
  hcaconfig[dbc, dbd, orgIndex],
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable]
)

##
## Open log file
##
log_open("integrated-customer-segmentation")

sep("<run.R> Sourcing pipeline files")

##
## source files
##
source("inst/pipeline/integrated/customer_segments/src/buildSegments.R")

##
## Process pipeline org by org
##
org_vec <- orgIndex()[(curr_client), org_uuid]

sep("<run.R> Starting Segmentation")
segments <- buildSegments(org_vec)
sep("<run.R> Completed Segmentation")

##
## Save output dataset
##
tbnam <- "customer_segments"
sep("<run.R> Saving to Integrated")
cn <- dbc(Sys.getenv("HCA_ENV", "prod2"), "integrated")
dbBegin(cn)
dbExecute(cn, paste("TRUNCATE TABLE", tbnam))
dbWriteTable(cn, tbnam, OUT, append = TRUE)
dbCommit(cn)
dbd(cn)


##
## Close log file
##
log_close()
