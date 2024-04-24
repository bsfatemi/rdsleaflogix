# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.
# Libraries ---------------------------------------------------------------
box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  dplyr[anti_join, arrange, bind_rows, count, distinct, filter, left_join, mutate, pull, select],
  hcasweed[build_sweed_customers],
  hcaconfig[dbc, dbd],
  lubridate[now],
  pipelinetools[db_read_table_unique]
)

# Vars --------------------------------------------------------------------
org <- "apothecarium"

# Run ---------------------------------------------------------------------
pg <- dbc("prod2", "cabbage_patch")
sweed_customers <- db_read_table_unique(
  pg, "apothecarium_sweed_customers", "CustomerId", "run_date_utc"
)
dbd(pg)

pg <- dbc("prod2", "consolidated")
akerna_customers <- dbReadTable(pg, "apothecarium_akerna_customers")
dbd(pg)

sweed_customers <- build_sweed_customers(sweed_customers, org)

# Join --------------------------------------------------------------------
# Discard Akerna phone numbers that are repeated in multiple Sweed customers.
sweed_multiple_phone_customers <- count(sweed_customers, phone) |>
  filter(n > 1) |>
  pull(phone)
# We will merge by `phone`, so remove duplicates by `last_updated_utc`.
akerna_customers <- arrange(akerna_customers, desc(last_updated_utc)) |>
  filter(!is.na(phone), !phone %in% sweed_multiple_phone_customers) |>
  distinct(phone, .keep_all = TRUE) |>
  mutate(
    customer_id = paste0("akerna_", customer_id),
    akerna_customer_id = customer_id
  )
# For each joined customer in Akerna, give their Sweed `customer_id`.
customers <- left_join(
  sweed_customers, select(akerna_customers, akerna_customer_id, phone),
  by = "phone"
)
# Bind Sweed customers that couldn't be joined by phone number.
customers <- bind_rows(
  customers, anti_join(akerna_customers, customers, by = "phone")
)

customers$run_date_utc <- now("UTC")
customers$geocode_type <- "none"
customers$pos_is_subscribed <- FALSE # client request
customers$org <- org

keep_cols <- c(
  "org", "source_system", "customer_id", "akerna_customer_id", "created_at_utc", "last_updated_utc",
  "source_run_date_utc", "phone", "first_name", "last_name", "full_name", "gender", "birthday",
  "age", "email", "long_address", "address_street1", "address_street2", "city", "state", "zipcode",
  "pos_is_subscribed", "user_type", "geocode_type", "run_date_utc", "loyalty_pts", "tags"
)

customers <- select(customers, {{ keep_cols }})

# Write -------------------------------------------------------------------
pg <- dbc("prod2", "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", "apothecarium_customers"))
dbWriteTable(pg, "apothecarium_customers", customers, append = TRUE)
dbCommit(pg)
dbd(pg)
