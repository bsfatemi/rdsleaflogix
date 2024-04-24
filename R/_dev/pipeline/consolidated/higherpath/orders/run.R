# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org has multiple POS in use sequentially (for different stores).

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  dplyr[bind_rows, filter, mutate],
  hcaconfig[dbc, dbd, get_org_stores, orgTimeZone],
  hcasquare[build_square_orders],
  hcatreez[build_trz2_orders],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "higherpath"
org_uuid <- "174ba423-593e-4431-9e4d-a912cdac3362"
org_stores <- get_org_stores(org_uuid)
treez_stores <- filter(org_stores, main_pos == "treez")
square_stores <- filter(org_stores, main_pos == "square")
tz <- orgTimeZone(org_uuid)
out_table <- paste0(org, "_orders")

# Run -------------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
treez_orders <- map_dfr(seq_len(nrow(treez_stores)), function(i) {
  store <- treez_stores[i, ]
  db_read_table_unique(
    pg, paste(org, store$facility, "trz2_tickets", sep = "_"), "order_number", "run_date_utc"
  ) |>
    build_trz2_orders(org, store$full_name, store$facility, tz, store$store_uuid) |>
    mutate(customer_id = paste0(store$facility, "-", customer_id))
})
square_orders <- map_dfr(seq_len(nrow(square_stores)), function(i) {
  store <- square_stores[i, ]
  orders <- db_read_table_unique(
    pg, paste(org, store$facility, "square_orders", sep = "_"), "id", "run_date_utc", columns = c(
      "id", "run_date_utc", "state", "customer_id", "created_at", "total_tax_money_amount",
      "total_money_amount", "total_discount_money_amount", "source_name", "location_id",
      "total_tip_money", "tenders"
    )
  )
  locations <- db_read_table_unique(
    pg, paste(org, store$facility, "square_locations", sep = "_"), "id", "run_date_utc",
    columns = c("id", "name", "run_date_utc")
  )
  orders <- build_square_orders(
    orders, locations, org, store$facility, store$full_name, store$store_uuid, tz
  )
  # Keep the provided store name.
  orders$order_facility <- store$full_name
  orders
})
dbd(pg)

# Bind Treez and Square orders.
orders <- bind_rows(treez_orders, square_orders)

# Write -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbCommit(pg)
dbd(pg)
