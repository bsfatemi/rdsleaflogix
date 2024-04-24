# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcawebjoint[build_wj_orders],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------
org <- args$org_short_name
stores <- args$stores_short_names[[1]]
stores_full_names <- args$stores_full_names[[1]]
stores_uuids <- args$stores_uuids[[1]]
out_table <- args$orders

# Run ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- map_dfr(seq_along(stores), function(i) {
  store <- stores[[i]]
  orders <- db_read_table_unique(
    pg, paste(org, store, "wj_orders", sep = "_"), "id", "run_date_utc"
  )
  deliveryzones <- db_read_table_unique(
    pg, paste(org, store, "wj_deliveryzones", sep = "_"), "id", "run_date_utc"
  )
  staff <- db_read_table_unique(
    pg, paste(org, store, "wj_staff", sep = "_"), "id", "run_date_utc"
  )
  build_wj_orders(
    orders, deliveryzones, staff, org, stores_full_names[[i]], store, stores_uuids[[i]]
  )
})
dbd(pg)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbCommit(pg)
dbd(pg)
