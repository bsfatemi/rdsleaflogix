# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org has multiple POS in use sequentially (for different stores).

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  dplyr[bind_rows, filter],
  hcablaze[build_blaze_orders],
  hcaconfig[dbc, dbd, get_org_stores, orgTimeZone],
  hcameadow[build_meadow_orders],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr, transpose]
)

# Vars ------------------------------------------------------------------------
org <- "cookies"
org_uuid <- "04b4d115-a955-4033-81cd-fb9be423b618"
org_stores <- get_org_stores(org_uuid)
blaze_stores <- filter(org_stores, main_pos == "blaze")
meadow_stores <- filter(org_stores, main_pos == "meadow")
tz <- orgTimeZone(org_uuid)
out_table <- paste0(org, "_orders")

# Run -------------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
blaze_orders <- map_dfr(transpose(blaze_stores), function(store) {
  transactions <- db_read_table_unique(
    pg, paste(org, store$facility, "blaze_transactions", sep = "_"), "id", "run_date_utc"
  )
  employees <- db_read_table_unique(
    pg, paste(org, store$facility, "blaze_employees", sep = "_"), "id", "run_date_utc"
  )
  build_blaze_orders(
    transactions, employees, org, store$full_name, store$facility, store$store_uuid
  )
})
meadow_orders <- map_dfr(transpose(meadow_stores), function(store) {
  db_read_table_unique(
    pg, paste(org, store$facility, "meadow_orders", sep = "_"), "id", "run_date_utc"
  ) |>
    build_meadow_orders(org, store$facility, store$full_name, store$store_uuid, tz)
})
dbd(pg)

# Bind Blaze and Meadow orders.
orders <- bind_rows(blaze_orders, meadow_orders)

# Write -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbCommit(pg)
dbd(pg)
