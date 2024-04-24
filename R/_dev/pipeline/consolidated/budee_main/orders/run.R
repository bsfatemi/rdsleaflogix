# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbExecute, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcabudee[build_budee_orders],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------
org <- args$org_short_name
stores <- args$stores_short_names[[1]]
stores_uuid <- args$stores_uuids[[1]]
tz <- args$tz
out_table <- args$orders

# Run ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- map_dfr(seq_along(stores), function(i) {
  store <- stores[[i]]
  deliveries <- db_read_table_unique(
    pg, paste(org, store, "budee_deliveries", sep = "_"), "id", "run_date_utc"
  )
  depots <- db_read_table_unique(
    pg, paste(org, store, "budee_depots", sep = "_"), "id", "run_date_utc"
  )
  build_budee_orders(deliveries, depots, org, store, stores_uuid[[i]], tz)
})
dbd(pg)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbd(pg)
