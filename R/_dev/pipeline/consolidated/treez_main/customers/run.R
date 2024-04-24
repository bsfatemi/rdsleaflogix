# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbExistsTable, dbWriteTable],
  dplyr[distinct, mutate],
  hcaconfig[dbc, dbd],
  hcatreez[build_trz2_customers],
  lubridate[days, today],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- args$org_short_name
stores <- args$stores_short_names[[1]]
update_day <- format(today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1)))
out_table <- args$customers

# Run -------------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
customers <- map_dfr(stores, function(store) {
  raw_customers <- db_read_table_unique(
    pg, paste(org, store, "trz2_customers", sep = "_"), "customer_id", "run_date_utc",
    extra_filters = paste0("run_date_utc >= '", update_day, "'")
  )
  # If no new cabbage patch customers return NULL so wont error on build.
  if (nrow(raw_customers) == 0) {
    return(NULL)
  }
  raw_customers |>
    build_trz2_customers(org) |>
    mutate(customer_id = paste0(store, "-", customer_id))
})
dbd(pg)

# Write -------------------------------------------------------------------
# Only have to delete and append if there's new data.
if (nrow(customers) > 0) {
  pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
  temp_table_name <- paste(org, "customers_temp", sep = "_")
  dbWriteTable(pg, temp_table_name, distinct(customers, customer_id), temporary = TRUE)
  dbBegin(pg)
  if (dbExistsTable(pg, out_table)) {
    dbExecute(pg, paste0(
      'DELETE FROM "', out_table, '" WHERE customer_id IN (SELECT customer_id FROM "',
      temp_table_name, '")'
    ))
  }
  dbWriteTable(pg, out_table, customers, append = TRUE)
  dbCommit(pg)
  dbd(pg)
}
