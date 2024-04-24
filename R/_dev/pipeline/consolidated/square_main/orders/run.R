# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcasquare[build_square_orders],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------
org <- args$org_short_name
stores <- args$stores_short_names[[1]]
stores_full_name <- args$stores_full_names[[1]]
stores_uuid <- args$stores_uuids[[1]]
tz <- args$tz
out_table <- args$orders

# Run ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- map_dfr(seq_along(stores), function(i) {
  store <- stores[[i]]
  orders <- db_read_table_unique(
    pg, paste(org, store, "square_orders", sep = "_"), "id", "run_date_utc", columns = c(
      "id", "run_date_utc", "state", "customer_id", "created_at", "total_tax_money_amount",
      "total_money_amount", "total_discount_money_amount", "source_name", "location_id",
      "total_tip_money", "tenders"
    )
  )
  locations <- db_read_table_unique(
    pg, paste(org, store, "square_locations", sep = "_"), "id", "run_date_utc",
    columns = c("id", "name", "run_date_utc")
  )
  build_square_orders(orders, locations, org, store, stores_full_name[[i]], stores_uuid[[i]], tz)
})
dbd(pg)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbCommit(pg)
dbd(pg)
