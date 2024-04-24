# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcaflowhub[build_flowhub_orders],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ---------------------------------------------------------------------
org <- args$org_short_name
stores <- args$stores_short_names[[1]]
stores_name <- args$stores_full_names[[1]]
stores_uuid <- args$stores_uuids[[1]]
tz <- args$tz
out_table <- args$orders

# Read ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- map_dfr(seq_along(stores), function(i) {
  store <- stores[[i]]
  db_read_table_unique(pg, paste(org, store, "flowhub_orders", sep = "_"), "id", "run_date_utc") |>
    build_flowhub_orders(tz, org, store, stores_name[[i]], stores_uuid[[i]]) |>
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
