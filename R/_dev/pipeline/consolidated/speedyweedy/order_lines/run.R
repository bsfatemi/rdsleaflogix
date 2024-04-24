# Consolidated order_lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

# Source ------------------------------------------------------------------
library(hcaconfig)
library(pipelinetools)
library(dplyr)
library(tidyr)
library(purrr)
library(stringr)
library(RPostgres)
library(lubridate)
library(jsonlite)
library(DBI)
source("inst/pipeline/consolidated/speedyweedy/order_lines/src/build_io_flatfile_order_lines.R")

# Run ---------------------------------------------------------------------
org <- "speedyweedy"
org_uuid <- lookupOrgGuid(org)
stores <- c("encino", "santa", "vista")
cats_map <- dplyr::select(pipelinetools::get_categories_mapping(org_uuid), -run_date_utc)
out_table <- paste0(org, "_order_lines")

### READ FROM CABBAGE_PATCH
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
order_lines <- map_dfr(stores, function(store) {
  raw_orders <- DBI::dbReadTable(pg, paste(org, store, "io_orders_products", sep = "_"))
  patients <- db_read_table_unique(
    pg, paste(org, store, "io_patients", sep = "_"), "patient_id", "run_date_utc"
  )

  order_lines <- build_io_flatfile_order_lines(raw_orders, patients, cats_map, org) |>
    mutate(customer_id = paste0(store, "-", customer_id))
})
dbd(pg)
### READ FROM CONSOLIDATED

order_lines$run_date_utc <- lubridate::now("UTC")

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
