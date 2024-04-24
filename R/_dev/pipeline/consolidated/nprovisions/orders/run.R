# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[bind_rows, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd, lookupOrgGuid, orgTimeZone, get_org_stores],
  hcapipelines[plIndex],
  hcaposabit[build_posabit_orders],
  lubridate[now],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------

org <- "nprovisions"
org_uuid <- lookupOrgGuid(org)
stores <- get_org_stores(org_uuid)$facility
tz <- orgTimeZone(org_uuid)
stores_name <- get_org_stores(org_uuid)$full_name
stores_uuid <- get_org_stores(org_uuid)$store_uuid
out_table <- plIndex()[short_name == org]$orders
# Run ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
posabit_orders <- map_dfr(seq_along(stores), function(i) {
  store <- stores[[i]]
  orders <- db_read_table_unique(
    pg, paste(org, store, "posabit_orders", sep = "_"), "id", "run_date_utc"
  )
  employees <- db_read_table_unique(
    pg, paste(org, store, "posabit_employees", sep = "_"), "id", "run_date_utc"
  )
  build_posabit_orders(orders, employees, org, store, stores_name[[i]], stores_uuid[[i]], tz)
})
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
customers <- dbGetQuery(
  pg, paste0("SELECT DISTINCT treez_customer_id, customer_id FROM ", org, "_customers")
)

treez_orders <- dbReadTable(pg, "nprovisions_treez_orders")
dbd(pg)


# Match treez customer IDs to the new IDs.
treez_orders <- rename(treez_orders, treez_customer_id = customer_id) |>
  mutate(treez_customer_id = paste0("treez_", treez_customer_id)) |>
  left_join(customers, by = "treez_customer_id") |>
  select(-treez_customer_id)

# Join ------------------------------------------------------------------------
orders <- bind_rows(posabit_orders, treez_orders)
orders$run_date_utc <- now("UTC")

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbCommit(pg)
dbd(pg)
