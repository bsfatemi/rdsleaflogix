# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[
    anti_join, bind_rows, distinct, if_else, left_join, mutate, one_of, rename, select
  ],
  glue[glue],
  hcablaze[build_blaze_orders],
  hcaconfig[dbc, dbd],
  lubridate[as_datetime, minutes],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr],
  randomNames[randomNames]
)

(db_env <- Sys.getenv("HCA_ENV", "prod2"))

# Vars --------------------------------------------------------------------
org <- "demoorg"
stores <- c("hollywood", "longbeach", "marinadelrey", "noho")
stores_full_names <- c("Hollywood", "Long Beach", "Marina Del Rey", "North Hollywood")
stores_uuids <- c(
  "90475f7a-dfd0-47b1-9ee2-511f98db0ce7", "8e33662d-844c-4d5d-8cec-021f800bf9bb",
  "d66051aa-118f-411b-bcc0-b674a42312c6", "c8daf842-e97b-4ba1-8191-0c1a3eecec61"
)

# Run ---------------------------------------------------------------------
pg <- dbc(db_env, "cabbage_patch")
orders <- map_dfr(seq_along(stores), function(i) {
  store <- stores[[i]]
  transactions <- db_read_table_unique(
    pg, paste(org, store, "blaze_transactions", sep = "_"), "id", "run_date_utc"
  )
  employees <- db_read_table_unique(
    pg, paste(org, store, "blaze_employees", sep = "_"), "id", "run_date_utc"
  )
  build_blaze_orders(transactions, employees, org, stores_full_names[[i]], store, stores_uuids[[i]])
})
# Read already anonymized data.
anonymized_customers <- dbGetQuery(pg, glue(
  "SELECT DISTINCT real_customer_id, customer_id FROM {org}_anonymized_customers"
))
anonymized_employees <- dbReadTable(pg, paste0(org, "_anonymized_employees"))
dbd(pg)

# Anonymize ---------------------------------------------------------------
# Get new (non-anonymized) employees.
employees_to_anonymize <- anti_join(
  distinct(orders, sold_by_id),
  anonymized_employees,
  by = c(sold_by_id = "real_sold_by_id")
)
employees_to_anonymize <- mutate(
  employees_to_anonymize,
  # Get random full names.
  sold_by = toupper(randomNames(nrow(employees_to_anonymize), name.sep = " "))
) |>
  rename(real_sold_by_id = sold_by_id)
# Create new customer IDs.
new_ids <- distinct(employees_to_anonymize, real_sold_by_id)
new_ids <- mutate(
  new_ids,
  sold_by_id = max(anonymized_employees$sold_by_id) + seq_len(nrow(new_ids))
)
employees_to_anonymize <- left_join(employees_to_anonymize, new_ids, by = "real_sold_by_id")

# Save newly anonymized data.
pg <- dbc(db_env, "cabbage_patch")
dbWriteTable(pg, paste0(org, "_anonymized_employees"), employees_to_anonymize, append = TRUE)
dbd(pg)

# Join the anonymized data.
anonymized_employees <- bind_rows(anonymized_employees, employees_to_anonymize)
# Replace real data with the anonymized data.
orders <- mutate(orders, real_customer_id = as.character(customer_id)) |>
  select(-one_of(setdiff(colnames(anonymized_customers), "real_customer_id"))) |>
  left_join(anonymized_customers, by = "real_customer_id") |>
  select(-real_customer_id)
orders <- mutate(orders, real_sold_by_id = as.character(sold_by_id)) |>
  select(-one_of(setdiff(colnames(anonymized_employees), "real_sold_by_id"))) |>
  left_join(anonymized_employees, by = "real_sold_by_id") |>
  select(-real_sold_by_id)

# Create fake order sources. Keep the existing ones, but replace Retail-Unknown for In-Store.
orders <- mutate(orders, order_source = if_else(
  order_type == "Retail" & order_source == "Unknown", "In-Store", order_source
))
# Delivery times should be cleaned up to be between 20 mins and 180 mins.
orders <- mutate(
  orders,
  # Max of 180 minutes.
  delivery_order_complete_local = if_else(
    order_type == "Delivery" &
      difftime(delivery_order_complete_local, delivery_order_received_local, units = "mins") > 180,
    as.character(as_datetime(delivery_order_received_local) + minutes(180)),
    delivery_order_complete_local
  ),
  # Min of 20 minutes.
  delivery_order_complete_local = if_else(
    order_type == "Delivery" &
      difftime(delivery_order_complete_local, delivery_order_received_local, units = "mins") < 20,
    as.character(as_datetime(delivery_order_received_local) + minutes(20)),
    delivery_order_complete_local
  )
)

# Last tweaks.
orders$customer_id <- as.character(orders$customer_id)
orders$sold_by_id <- as.character(orders$sold_by_id)

# Write -------------------------------------------------------------------
pg <- dbc(db_env, "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", paste(org, "orders", sep = "_")))
dbWriteTable(pg, paste(org, "orders", sep = "_"), orders, append = TRUE)
dbCommit(pg)
dbd(pg)
