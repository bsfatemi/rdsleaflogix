library(hcaconfig)
library(pipelinetools)
library(dplyr)
library(tidyr)
library(purrr)
library(stringr)
library(RPostgres)
library(DBI)
library(lubridate)
library(jsonlite)
library(janitor)
library(anytime)

source("inst/pipeline/consolidated/speedyweedy/customers/src/build_io_flatfile_customers.R")
# Vars ---------------------------------------------------------------------
org <- "speedyweedy"
stores <- c("encino", "santa", "vista")
out_table <- paste0(org, "_customers")

# Read ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
customers <- map_dfr(stores, function(store) {
  customers <- db_read_table_unique(
    pg, paste(org, store, "io_patients", sep = "_"), "patient_id", "run_date_utc"
  )

  customers <- build_io_flatfile_customers(customers, org) |>
    mutate(customer_id = paste0(store, "-", customer_id))
})
dbd(pg)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbCommit(pg)
dbd(pg)
