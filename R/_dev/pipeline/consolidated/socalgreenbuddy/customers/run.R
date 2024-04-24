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
  hcameadow[build_meadow_customers],
  lubridate[now],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------
org <- "socalgreenbuddy"
stores <- get_org_stores(lookupOrgGuid(org))$facility
out_table <- "socalgreenbuddy_customers"

# Run ---------------------------------------------------------------------
pg <- hcaconfig::dbc("prod2", "consolidated")
io_customers <- dbReadTable(pg, "socalgreenbuddy_io_customers")
hcaconfig::dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
meadow_customers <- map_dfr(stores, function(store) {
  customers <- db_read_table_unique(
    pg, paste(org, store, "meadow_customers", sep = "_"), "id", "run_date_utc"
  )
  build_meadow_customers(customers, org)
})
dbd(pg)
meadow_multiple_phone_customers <- count(meadow_customers, phone) |>
  filter(n > 1) |>
  pull(phone)
# We will merge by `phone`, so remove duplicates, by `last_updated_utc`.
io_customers <- arrange(io_customers, desc(last_updated_utc)) |>
  filter(!is.na(phone), !phone %in% meadow_multiple_phone_customers) |>
  distinct(phone, .keep_all = TRUE) |>
  mutate(
    customer_id = paste0("io_", customer_id),
    io_customer_id = customer_id
  )
# For each joined customer in io, give their meadow `customer_id`.
customers <- left_join(
  meadow_customers, select(io_customers, io_customer_id, phone),
  by = "phone"
)
# Bind io customers that couldn't be joined by phone number.
customers <- bind_rows(customers, anti_join(io_customers, customers, by = "phone"))
customers$run_date_utc <- now("UTC")
# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbCommit(pg)
dbd(pg)
