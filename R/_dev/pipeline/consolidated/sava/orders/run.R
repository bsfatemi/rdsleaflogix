# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[bind_rows, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd, lookupOrgGuid, orgTimeZone],
  hcamagento[build_magento_orders],
  lubridate[now],
  pipelinetools[db_read_table_unique],
  stringr[str_glue],
)

# Vars ------------------------------------------------------------------
org <- "sava"
store <- "main"
order_facility <- "SAVA"
tz <- orgTimeZone(lookupOrgGuid(org))

# Read ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
magento_orders <- db_read_table_unique(
  pg, paste(org, "magento_orders", sep = "_"), "order_id", "run_date_utc"
)
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
customers <- dbGetQuery(pg, str_glue(
  "SELECT DISTINCT blz_customer_id, customer_id FROM {org}_customers"
))
blz_orders <- dbReadTable(pg, "sava_blaze_orders")
dbd(pg)


# Run ---------------------------------------------------------------------
# For Blaze ------
# Match Blaze customer IDs to the new IDs.
blz_orders <- rename(blz_orders, blz_customer_id = customer_id) |>
  mutate(blz_customer_id = paste0("blz_", blz_customer_id)) |>
  left_join(customers, by = "blz_customer_id") |>
  select(-blz_customer_id)
# For Magento ------
magento_orders <- build_magento_orders(magento_orders, org, store, order_facility, tz = tz)

# Join --------------------------------------------------------------------
orders <- bind_rows(magento_orders, blz_orders)
orders$run_date_utc <- now("UTC")

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", paste0(org, "_orders")))
dbWriteTable(pg, paste0(org, "_orders"), orders, append = TRUE)
dbCommit(pg)
dbd(pg)
