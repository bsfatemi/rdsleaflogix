# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  dplyr[mutate, if_else],
  hcaconfig[dbc, dbd, get_org_stores, lookupOrgGuid],
  hcasquare[build_square_customers],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr],
  hcapipelines[plIndex]
)

# Vars ------------------------------------------------------------------
org <- "blomshop"
stores <- get_org_stores(lookupOrgGuid(org))$facility
out_table <- plIndex()[short_name == org]$customers

# Run ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
customers <- map_dfr(stores, function(store) {
  customers <- db_read_table_unique(
    pg, paste(org, store, "square_customers", sep = "_"), "id", "run_date_utc",
    columns = c(
      "id", "run_date_utc", "segment_ids", "created_at", "updated_at", "phone_number", "given_name",
      "family_name", "birthday", "email_address", "address_line_1", "address_line_2", "locality",
      "administrative_district_level_1", "postal_code", "email_unsubscribed"
    )
  )
  customers_segments <- db_read_table_unique(
    pg, paste(org, store, "square_customers_segments", sep = "_"), "id", "run_date_utc",
    columns = c("id", "run_date_utc", "name")
  )
  loyalty_accounts <- db_read_table_unique(
    pg, paste(org, store, "square_loyalty_accounts", sep = "_"), "id", "run_date_utc",
    columns = c("id", "run_date_utc", "customer_id", "balance")
  )
  # customer says they use the loyalty program as a way to ask for sms consent
  customers <- mutate(customers, email_unsubscribed = if_else(
    id %in% loyalty_accounts$customer_id, FALSE, email_unsubscribed
  ))
  build_square_customers(customers, customers_segments, loyalty_accounts, org)
})
dbd(pg)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbCommit(pg)
dbd(pg)
