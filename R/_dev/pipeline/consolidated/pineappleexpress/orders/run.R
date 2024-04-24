# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[bind_rows, left_join, mutate, rename, select],
  hcablaze[build_blaze_orders],
  hcaconfig[dbc, dbd, get_store_id, lookupOrgGuid, orgTimeZone, get_org_stores],
  lubridate[now],
  pipelinetools[db_read_table_unique],
  hcapipelines[plIndex],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "pineappleexpress"
org_uuid <- lookupOrgGuid(org)
stores <- get_org_stores(lookupOrgGuid(org))$facility
tz <- orgTimeZone(org_uuid)
stores_full_names <- get_org_stores(lookupOrgGuid(org))$full_name
stores_uuids <- get_org_stores(lookupOrgGuid(org))$store_uuid
out_table <- plIndex()[short_name == org]$orders
# Run -------------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
customers <- dbGetQuery(
  pg, paste0("SELECT DISTINCT treez_customer_id, customer_id FROM ", org, "_customers")
)
treez_orders <- dbReadTable(pg, "pineappleexpress_treez_orders")
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
blaze_orders <- map_dfr(seq_along(stores), function(i) {
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

# Match Blaze customer IDs to the new IDs.
treez_orders <- rename(treez_orders, treez_customer_id = customer_id) |>
  mutate(treez_customer_id = paste0("treez_", treez_customer_id)) |>
  left_join(customers, by = "treez_customer_id") |>
  select(-treez_customer_id)

# Join ------------------------------------------------------------------------
orders <- bind_rows(blaze_orders, treez_orders)
orders$run_date_utc <- now("UTC")

# Write -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", paste0(org, "_orders")))
dbWriteTable(pg, paste0(org, "_orders"), orders, append = TRUE)
dbCommit(pg)
dbd(pg)
