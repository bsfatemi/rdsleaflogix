# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[bind_rows, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd, get_store_id, lookupOrgGuid, orgTimeZone, get_org_stores],
  hcameadow[build_meadow_orders],
  hcapipelines[plIndex],
  lubridate[now],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------
org <- "socalgreenbuddy"
org_uuid <- lookupOrgGuid(org)
stores <- get_org_stores(lookupOrgGuid(org))$facility
tz <- orgTimeZone(org_uuid)
store_full_name <- get_org_stores(lookupOrgGuid(org))$full_name
stores_uuid <- get_org_stores(lookupOrgGuid(org))$store_uuid
out_table <- plIndex()[short_name == org]$orders


# Run ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
customers <- dbGetQuery(
  pg, paste0("SELECT DISTINCT io_customer_id, customer_id FROM ", org, "_customers")
)

io_orders <- dbReadTable(pg, "socalgreenbuddy_io_orders")
dbd(pg)


pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
meadow_orders <- map_dfr(seq_along(stores), function(i) {
  store <- stores[[i]]
  db_read_table_unique(pg, paste(org, store, "meadow_orders", sep = "_"), "id", "run_date_utc") |>
    build_meadow_orders(org, store, store_full_name[[i]], stores_uuid[[i]], tz)
})
dbd(pg)

io_orders <- rename(io_orders, io_customer_id = customer_id) |>
  mutate(io_customer_id = paste0("io_", io_customer_id)) |>
  left_join(customers, by = "io_customer_id") |>
  select(-io_customer_id)

# Join ------------------------------------------------------------------------
orders <- bind_rows(meadow_orders, io_orders)
orders$run_date_utc <- now("UTC")

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbCommit(pg)
dbd(pg)
