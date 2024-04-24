# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

# Source ------------------------------------------------------------------
library(hcaconfig)
library(pipelinetools)
library(dplyr)
library(tidyr)
library(purrr)
library(DBI)
library(stringr)
library(RPostgres)
library(lubridate)
library(jsonlite)
source(
  "inst/pipeline/consolidated/speedyweedy/orders/src/build_io_flatfile_orders.R"
)

org <- "speedyweedy"
stores <- c("Encino" = "encino", "Santa Ana" = "santa", "Vista" = "vista")
out_table <- paste0(org, "_orders")


# Read ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- map_dfr(names(stores), function(store_name) {
  store <- stores[[store_name]]
  raw_orders <- DBI::dbReadTable(pg, paste(org, store, "io_orders_products", sep = "_"))
  customers <- db_read_table_unique(
    pg, paste(org, store, "io_patients", sep = "_"), "patient_id", "run_date_utc"
  )

  orders <- build_io_flatfile_orders(raw_orders, customers, org, store, store_name) |>
    mutate(customer_id = paste0(store, "-", customer_id))
})
dbd(pg)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbCommit(pg)
dbd(pg)
