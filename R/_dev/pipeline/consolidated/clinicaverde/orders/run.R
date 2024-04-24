# Consolidated Orders
# init.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

# Libraries ---------------------------------------------------------------
box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  hcaconfig[dbc, dbd, lookupOrgGuid, get_org_stores, orgTimeZone],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr, transpose]
)

# Source ------------------------------------------------------------------
source("inst/pipeline/consolidated/clinicaverde/orders/src/build_akerna_orders.R", local = TRUE)

# Run ---------------------------------------------------------------------
org <- "clinicaverde"
org_uuid <- lookupOrgGuid(org)
stores <- get_org_stores(org_uuid)
tzone <- orgTimeZone(org_uuid)
db_env <- Sys.getenv("HCA_ENV", "prod2")
pg <- dbc(db_env, "cabbage_patch")
addresses <- db_read_table_unique(
  pg, paste0(org, "_all_akerna_consumers_addresses"), "consumer_id", "updated_at"
)
orders <- map_dfr(transpose(stores), function(store) {
  raw <- db_read_table_unique(
    pg, paste0(org, "_", store$facility, "_akerna_orders"), "id", "run_date_utc"
  )
  build_akerna_orders(
    raw, addresses, org, order_facility = store$full_name, facility = store$facility, tz = tzone,
    store_id = store$store_uuid
  )
})
dbd(pg)

# Write -------------------------------------------------------------------
con <- dbc(db_env, "consolidated")
dbBegin(con)
dbExecute(con, paste("TRUNCATE TABLE", paste0(org, "_orders")))
dbWriteTable(con, paste0(org, "_orders"), orders, append = TRUE)
dbCommit(con)
dbd(con)
