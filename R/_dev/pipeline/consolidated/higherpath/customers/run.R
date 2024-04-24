# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org has multiple POS in use sequentially (for different stores).

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  dplyr[bind_rows, filter, group_by, mutate, ungroup, row_number],
  hcaconfig[dbc, dbd, get_org_stores],
  hcasquare[build_square_customers],
  hcatreez[build_trz2_customers],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "higherpath"
org_uuid <- "174ba423-593e-4431-9e4d-a912cdac3362"
org_stores <- get_org_stores(org_uuid)
treez_stores <- filter(org_stores, main_pos == "treez")$facility
square_stores <- filter(org_stores, main_pos == "square")$facility
out_table <- paste0(org, "_customers")

# Run -------------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
treez_customers <- map_dfr(treez_stores, function(store) {
  db_read_table_unique(
    pg, paste(org, store, "trz2_customers", sep = "_"), "customer_id", "run_date_utc"
  ) |>
    build_trz2_customers(org) |>
    mutate(customer_id = paste0(store, "-", customer_id))
})
square_customers <- map_dfr(square_stores, function(store) {
  customers <- db_read_table_unique(
    pg, paste(org, store, "square_customers", sep = "_"), "id", "run_date_utc", columns = c(
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
  build_square_customers(customers, customers_segments, loyalty_accounts, org)
})
dbd(pg)

# Bind Treez and Square customers.
customers <- bind_rows(treez_customers, square_customers)
customers$run_date_utc <- max(customers$run_date_utc, na.rm = TRUE)

# Write -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbCommit(pg)
dbd(pg)
