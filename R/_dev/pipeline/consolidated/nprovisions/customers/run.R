# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  dplyr[
    anti_join, arrange, bind_rows, count, desc, distinct, filter, left_join, mutate, pull, select
  ],
  hcaconfig[dbc, dbd, get_org_stores, lookupOrgGuid],
  hcapipelines[plIndex],
  hcaposabit[build_posabit_customers],
  lubridate[now],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr],
)

# Vars ------------------------------------------------------------------
org <- "nprovisions"
stores <- get_org_stores(lookupOrgGuid(org))$facility
out_table <- plIndex()[short_name == org]$customers

# Run ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
posabit_customers <- map_dfr(stores, function(store) {
  customers <- db_read_table_unique(
    pg, paste(org, store, "posabit_customers", sep = "_"), "id", "run_date_utc"
  )
  build_posabit_customers(customers, org)
})
dbd(pg)


pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
treez_customers <- dbReadTable(pg, "nprovisions_treez_customers")
dbd(pg)

posabit_multiple_phone_customers <- count(posabit_customers, phone) |>
  filter(n > 1) |>
  pull(phone)
# We will merge by `phone`, so remove duplicates, by `last_updated_utc`.
treez_customers <- arrange(treez_customers, desc(last_updated_utc)) |>
  filter(!is.na(phone), !phone %in% posabit_multiple_phone_customers) |>
  distinct(phone, .keep_all = TRUE) |>
  mutate(
    customer_id = paste0("treez_", customer_id),
    treez_customer_id = customer_id
  )
# For each joined customer in treez, give their posabit `customer_id`.
customers <- left_join(
  posabit_customers, select(treez_customers, treez_customer_id, phone),
  by = "phone"
)
# Bind treez customers that couldn't be joined by phone number.
customers <- bind_rows(customers, anti_join(treez_customers, customers, by = "phone"))
customers$run_date_utc <- now("UTC")
# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbCommit(pg)
dbd(pg)
