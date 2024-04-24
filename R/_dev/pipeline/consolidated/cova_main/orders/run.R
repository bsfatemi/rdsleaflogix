# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcacova[build_cova_orders],
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
  invoices_summaries <- db_read_table_unique(
    pg, paste(org, store, "cova_invoices_summaries", sep = "_"), "id", "run_date_utc"
  )
  invoices <- db_read_table_unique(
    pg, paste(org, store, "cova_invoices", sep = "_"), "id", "run_date_utc"
  )
  build_cova_orders(invoices_summaries, invoices, org, store, stores_uuid[[i]], tz)
})
dbd(pg)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbCommit(pg)
dbd(pg)
