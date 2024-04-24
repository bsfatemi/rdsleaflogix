# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  dplyr[anti_join, arrange, bind_rows, desc, distinct, left_join, mutate, one_of, rename, select],
  hcablaze[build_blaze_customers],
  hcaconfig[dbc, dbd],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr],
  randomNames[randomNames]
)

(db_env <- Sys.getenv("HCA_ENV", "prod2"))

# Vars --------------------------------------------------------------------
org <- "demoorg"
stores <- c("hollywood", "longbeach", "marinadelrey", "noho")

# Run ---------------------------------------------------------------------
pg <- dbc(db_env, "cabbage_patch")
customers <- map_dfr(stores, function(store) {
  members <- db_read_table_unique(
    pg, paste(org, store, "blaze_members", sep = "_"), "id", "run_date_utc"
  )
  build_blaze_customers(members, org, store)
})
# Read already anonymized customers data.
anonymized_customers <- dbReadTable(pg, paste0(org, "_anonymized_customers"))
dbd(pg)

# De-dupe by customer_id.
customers <- arrange(customers, desc(last_updated_utc)) |>
  distinct(customer_id, .keep_all = TRUE)

# Anonymize ---------------------------------------------------------------
# Get new (non-anonymized) customers.
customers_to_anonymize <- anti_join(
  distinct(customers, customer_id, birthday), anonymized_customers,
  by = c(customer_id = "real_customer_id")
)

customers_to_anonymize <- mutate(
  customers_to_anonymize,
  # Get random full names.
  first_name = toupper(randomNames(nrow(customers_to_anonymize)))
) |>
  mutate(
    last_name = gsub(", .*", "", first_name),
    first_name = gsub(".*, ", "", first_name),
    full_name = paste(first_name, last_name),
    # The email will be the name plus the birthday.
    email = paste0(
      "emailtest+",
      gsub("( )|(-)", "", paste(first_name, last_name, birthday, sep = "_")),
      "@happycabbage.io"
    ),
    # The phone will be a random number with code "+1 420".
    phone = paste0("+1420", sample(1000000:9999999, nrow(customers_to_anonymize), replace = TRUE))
  ) |>
  rename(real_customer_id = customer_id)
# Anonymize (loyalty) tags.
new_tags <- c("Employee", "Neighbor", "Student", "Veteran", "VIP")
customers_to_anonymize$tags <- sample(new_tags, nrow(customers_to_anonymize), replace = TRUE)
# Create new customer IDs.
new_ids <- distinct(customers_to_anonymize, real_customer_id)
new_ids <- mutate(
  new_ids,
  customer_id = max(anonymized_customers$customer_id) + seq_len(nrow(new_ids))
)
customers_to_anonymize <- left_join(customers_to_anonymize, new_ids, by = "real_customer_id")

# Save newly anonymized customers.
pg <- dbc(db_env, "cabbage_patch")
dbWriteTable(pg, paste0(org, "_anonymized_customers"), customers_to_anonymize, append = TRUE)
dbd(pg)

# Join the anonymized customers.
anonymized_customers <- bind_rows(anonymized_customers, customers_to_anonymize)
# Replace real data with the anonymized data.
customers <- mutate(customers, real_customer_id = customer_id) |>
  select(-one_of(setdiff(colnames(anonymized_customers), "real_customer_id"))) |>
  left_join(anonymized_customers, by = "real_customer_id") |>
  select(-real_customer_id)

# Last tweaks.
customers$customer_id <- as.character(customers$customer_id)

# Write -------------------------------------------------------------------
pg <- dbc(db_env, "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", paste(org, "customers", sep = "_")))
dbWriteTable(pg, paste(org, "customers", sep = "_"), customers, append = TRUE)
dbCommit(pg)
dbd(pg)
