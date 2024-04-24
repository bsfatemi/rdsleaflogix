# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  dplyr[left_join, mutate, select],
  hcabiotrack[build_biotrack_orders],
  hcaconfig[dbc, dbd],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------
org <- "verano_muv"
stores <- "verano_muv"
stores_uuid <- "42042042-0420-4204-2042-042042042042"
tz <- "America/Chicago"
out_table <- "verano_muv_orders"

# Run ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- map_dfr(seq_along(stores), function(i) {
  store <- stores[[i]]
  orders <- db_read_table_unique(
    pg, paste(org, store, "biotrack_orders", sep = "_"), "id", "run_date_utc", columns = c(
      "customerid", "id", "is_a_delivery", "location_name", "modified_on", "price", "run_date_utc",
      "tax", "totalprice", "userid", "preorder"
    )
  )
  build_biotrack_orders(orders, org, store, stores_uuid[[i]], tz)
})
muv_stores <- dbReadTable(pg, "verano_muv_biotrack_stores")
dbd(pg)

orders <- left_join(
  select(orders, -store_id),
  select(muv_stores, order_facility = store, store_id = store_uuid),
  by = "order_facility"
) |>
  mutate(facility = tolower(gsub("\\s", "_", order_facility)))

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbCommit(pg)
dbd(pg)
