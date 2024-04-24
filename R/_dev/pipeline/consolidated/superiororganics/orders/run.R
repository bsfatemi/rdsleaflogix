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

# Run ---------------------------------------------------------------------
org <- "superiororganics"
org_uuid <- lookupOrgGuid(org)
store_full_name <- "Main"
store <- "main"
tz <- orgTimeZone(org_uuid)
store_uuid <- get_store_id(org_uuid, store)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
customers <- dbGetQuery(
  pg, paste0("SELECT DISTINCT prot_customer_id, customer_id FROM ", org, "_customers")
)
proteus_orders <- dbReadTable(pg, "superiororganics_proteus_orders")
dbd(pg)

pg <- dbc("prod2", "cabbage_patch")
treez_tickets <- db_read_table_unique(
  pg, paste(org, store, "trz2_tickets", sep = "_"), "ticket_id", "run_date_utc"
)
dbd(pg)

# Build.
treez_orders <- build_trz2_orders(treez_tickets, org, store_full_name, store, tz, store_uuid) |>
  mutate(customer_id = paste0(store, "-", customer_id))
treez_orders$source_run_date_utc <- as.character(treez_orders$source_run_date_utc)

# Match proteus customer IDs to the new IDs.
proteus_orders <- rename(proteus_orders, prot_customer_id = customer_id) |>
  mutate(prot_customer_id = paste0("proteus_", prot_customer_id)) |>
  left_join(customers, by = "prot_customer_id") |>
  select(-prot_customer_id)

# Join ------------------------------------------------------------------------
orders <- bind_rows(treez_orders, proteus_orders)
orders$run_date_utc <- now("UTC")

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", paste0(org, "_orders")))
dbWriteTable(pg, paste0(org, "_orders"), orders, append = TRUE)
dbCommit(pg)
dbd(pg)
