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
  hcamagento[build_magento_customers],
  lubridate[now],
  pipelinetools[db_read_table_unique],
)

# Vars ------------------------------------------------------------------
org <- "sava"
store <- "main"

# Read ---------------------------------------------------------------------
# Connect and Read In Data
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
magento_customers <- db_read_table_unique(
  pg, paste(org, "magento_customers", sep = "_"), "id", "run_date_utc"
)
dbd(pg)

# Run ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
blz_customers <- dbReadTable(pg, "sava_blaze_customers")
dbd(pg)

magento_customers <- build_magento_customers(magento_customers, org, store)

# Join --------------------------------------------------------------------
# Discard Blaze phone numbers that are repeated in multiple Magento customers.
magento_multiple_phone_customers <- count(magento_customers, phone) |>
  filter(n > 1) |>
  pull(phone)
# We will merge by `phone`, so remove duplicates by `last_updated_utc`.
blz_customers <- arrange(blz_customers, desc(last_updated_utc)) |>
  filter(!is.na(phone), !phone %in% magento_multiple_phone_customers) |>
  distinct(phone, .keep_all = TRUE) |>
  mutate(
    customer_id = paste0("blz_", customer_id),
    blz_customer_id = customer_id
  )
# For each joined customer in Blaze, give their Magento `customer_id`.
customers <- left_join(
  magento_customers, select(blz_customers, blz_customer_id, phone),
  by = "phone"
)
# Bind Magento customers that couldn't be joined by phone number.
customers <- bind_rows(
  customers, anti_join(blz_customers, customers, by = "phone")
)
customers$run_date_utc <- now("UTC")

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", "sava_customers"))
dbWriteTable(pg, "sava_customers", customers, append = TRUE)
dbCommit(pg)
dbd(pg)
