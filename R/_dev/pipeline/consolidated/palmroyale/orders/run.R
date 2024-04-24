# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[bind_rows, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd, get_store_id, lookupOrgGuid, orgTimeZone],
  hcatreez[build_trz2_orders],
  lubridate[now],
  pipelinetools[db_read_table_unique]
)

# Vars ------------------------------------------------------------------------
org <- "palmroyale"
org_uuid <- lookupOrgGuid(org)
store <- "main"
tz <- orgTimeZone(org_uuid)
store_full_name <- "Main"
store_uuid <- get_store_id(org_uuid, store)

# Run -------------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
customers <- dbGetQuery(
  pg, paste0("SELECT DISTINCT greenbits_customer_id, customer_id FROM ", org, "_customers")
)
greenbits_orders <- dbReadTable(pg, "palmroyale_greenbits_orders")
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
treez_tickets <- db_read_table_unique(
  pg, paste(org, store, "trz2_tickets", sep = "_"), "ticket_id", "run_date_utc"
)
dbd(pg)

treez_orders <- build_trz2_orders(treez_tickets, org, store_full_name, store, tz, store_uuid) |>
  mutate(customer_id = paste0(store, "-", customer_id))

# Match GreenBits customer IDs to the new IDs.
greenbits_orders <- rename(greenbits_orders, greenbits_customer_id = customer_id) |>
  mutate(greenbits_customer_id = paste0("greenbits_", greenbits_customer_id)) |>
  left_join(customers, by = "greenbits_customer_id") |>
  select(-greenbits_customer_id)

# Join ------------------------------------------------------------------------
orders <- bind_rows(treez_orders, greenbits_orders)
orders$run_date_utc <- now("UTC")

# Write -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", paste0(org, "_orders")))
dbWriteTable(pg, paste0(org, "_orders"), orders, append = TRUE)
dbCommit(pg)
dbd(pg)
