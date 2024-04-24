# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  dplyr[
    anti_join, arrange, bind_rows, count, desc, distinct, filter, left_join, mutate, pull, select
  ],
  hcaconfig[dbc, dbd],
  hcatreez[build_trz2_customers],
  lubridate[now],
  pipelinetools[db_read_table_unique]
)

# Vars ------------------------------------------------------------------------
org <- "palmroyale"
store <- "main"

# Run -------------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
treez_customers <- db_read_table_unique(
  pg, paste(org, store, "trz2_customers", sep = "_"), "customer_id", "run_date_utc"
)
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
greenbits_customers <- dbReadTable(pg, "palmroyale_greenbits_customers")
dbd(pg)
treez_customers <- build_trz2_customers(treez_customers, org) |>
  mutate(customer_id = paste0(store, "-", customer_id))

# Join --------------------------------------------------------------------
# Discard GreenBits phone numbers that are repeated in multiple Treez customers.
treez_multiple_phone_customers <- count(treez_customers, phone) |>
  filter(n > 1) |>
  pull(phone)
# We will merge by `phone`, so remove duplicates, by `last_updated_utc`.
greenbits_customers <- arrange(greenbits_customers, desc(last_updated_utc)) |>
  filter(!is.na(phone), !phone %in% treez_multiple_phone_customers) |>
  distinct(phone, .keep_all = TRUE) |>
  mutate(
    customer_id = paste0("greenbits_", customer_id),
    greenbits_customer_id = customer_id
  )
# For each joined customer in GreenBits, give their Treez `customer_id`.
customers <- left_join(
  treez_customers, select(greenbits_customers, greenbits_customer_id, phone),
  by = "phone"
)
# Bind GreenBits customers that couldn't be joined by phone number.
customers <- bind_rows(customers, anti_join(greenbits_customers, customers, by = "phone"))
customers$run_date_utc <- now("UTC")

# Write -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", paste(org, "customers", sep = "_")))
dbWriteTable(pg, paste(org, "customers", sep = "_"), customers, append = TRUE)
dbCommit(pg)
dbd(pg)
