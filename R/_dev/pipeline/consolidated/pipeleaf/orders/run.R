# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd, orgTimeZone, get_org_stores, lookupOrgGuid],
  hcaflowhub[build_flowhub_orders],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr],
  hcapipelines[plIndex]
)

# Vars ---------------------------------------------------------------------
org <- "pipeleaf"
org_uuid <- lookupOrgGuid(org)
org_stores <- get_org_stores(org_uuid)
stores <- org_stores$facility
stores_name <- org_stores$full_name
stores_uuid <- org_stores$store_uuid
tz <- orgTimeZone(org_uuid)
out_table <- plIndex()[short_name == org]$orders

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
