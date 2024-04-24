# Consolidated Customers
# init.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.
# Libraries ---------------------------------------------------------------
box::use(
  anytime[anydate],
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  dplyr[select],
  hcaconfig[dbc, dbd],
  pipelinetools[db_read_table_unique]
)

# Source ------------------------------------------------------------------
source(
  "inst/pipeline/consolidated/clinicaverde/customers/src/build_akerna_customers.R", local = TRUE
)
org <- "clinicaverde"
store <- "all"
db_env <- Sys.getenv("HCA_ENV", "prod2")

# Run ---------------------------------------------------------------------
pg <- dbc(db_env, "cabbage_patch")
consumers <- db_read_table_unique(
  pg, paste(org, store, "akerna_consumers", sep = "_"), "id", "run_date_utc"
)
addresses <- db_read_table_unique(
  pg, paste(org, store, "akerna_consumers_addresses", sep = "_"), "id", "run_date_utc"
)
dbd(pg)

customers <- build_akerna_customers(org, consumers, addresses)
customers$pos_is_subscribed <- TRUE # Need to ask Macey about this

keep_cols <- c(
  "org", "source_system",
  "customer_id", "created_at_utc", "last_updated_utc",
  "source_run_date_utc", "phone", "first_name", "last_name",
  "full_name", "gender", "birthday", "age", "email", "long_address",
  "address_street1", "address_street2", "city", "state", "zipcode",
  "pos_is_subscribed", "user_type", "geocode_type", "run_date_utc", "loyalty_pts"
)

customers <- select(customers, {{ keep_cols }})

# Write -------------------------------------------------------------------
pg <- dbc(db_env, "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", paste0(org, "_customers")))
dbWriteTable(pg, paste0(org, "_customers"), customers, append = TRUE)
dbCommit(pg)
dbd(pg)
