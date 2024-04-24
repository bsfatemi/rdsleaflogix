# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  hcablaze[build_blaze_orders],
  hcaconfig[dbc, dbd],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- args$org_short_name
stores <- args$stores_short_names[[1]]
stores_full_names <- args$stores_full_names[[1]]
stores_uuids <- args$stores_uuids[[1]]
out_table <- args$orders

# Run -------------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- map_dfr(seq_along(stores), function(i) {
  store <- stores[[i]]
  transactions <- db_read_table_unique(
    pg, paste(org, store, "blaze_transactions", sep = "_"), "id", "run_date_utc"
  )
  employees <- db_read_table_unique(
    pg, paste(org, store, "blaze_employees", sep = "_"), "id", "run_date_utc"
  )
  build_blaze_orders(transactions, employees, org, stores_full_names[[i]], store, stores_uuids[[i]])
})
dbd(pg)

# Write -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbCommit(pg)
dbd(pg)
