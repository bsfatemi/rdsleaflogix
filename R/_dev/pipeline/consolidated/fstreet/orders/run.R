# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbExistsTable, dbWriteTable],
  dplyr[distinct, mutate],
  hcaconfig[dbc, dbd, get_org_stores, lookupOrgGuid, orgTimeZone],
  hcatreez[build_trz2_orders],
  lubridate[days, today],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars --------------------------------------------------------------------
org <- "fstreet"
org_uuid <- lookupOrgGuid(org)
stores <- get_org_stores(org_uuid)$facility
stores_name <- get_org_stores(org_uuid)$full_name
stores_uuid <- get_org_stores(org_uuid)$store_uuid
tz <- orgTimeZone(org_uuid)
update_day <- format(today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1)))
out_table <- "fstreet_trz2_orders"

# Run ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- map_dfr(seq_along(stores), function(i) {
  store <- stores[[i]]
  raw_orders <- db_read_table_unique(
    pg, paste(org, store, "trz2_tickets", sep = "_"), "order_number", "run_date_utc",
    extra_filters = paste0("run_date_utc >= '", update_day, "'")
  )
  # If no new cabbage patch return NULL so wont error on build.
  if (nrow(raw_orders) == 0) {
    return(NULL)
  }
  build_trz2_orders(raw_orders, org, stores_name[[i]], store, tz, stores_uuid[[i]]) |>
    mutate(customer_id = paste0(store, "-", customer_id))
})
dbd(pg)

# Write -------------------------------------------------------------------
# Only have to delete and append if there's new data.
if (nrow(orders) > 0) {
  pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
  dbWriteTable(
    pg, paste(org, "orders_temp", sep = "_"), distinct(orders, order_id),
    temporary = TRUE
  )
  dbBegin(pg)
  if (dbExistsTable(pg, out_table)) {
    dbExecute(pg, paste0(
      'DELETE FROM "', out_table, '" WHERE order_id IN (SELECT order_id FROM "',
      paste(org, "orders_temp", sep = "_"), '")'
    ))
  }
  dbWriteTable(pg, out_table, orders, append = TRUE)
  dbCommit(pg)
  dbd(pg)
}
