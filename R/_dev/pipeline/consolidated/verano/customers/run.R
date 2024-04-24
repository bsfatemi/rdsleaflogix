# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org has multiple POS in use sequentially (for different stores).

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbWriteTable],
  dplyr[bind_rows, distinct, filter, group_by, left_join, mutate, row_number, select, ungroup],
  hcaconfig[dbc, dbd, get_org_stores],
  hcaleaflogix[build_ll_customers],
  hcatreez[build_trz2_customers],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "verano"
org_uuid <- "a6cefdc6-0561-48ee-88cf-7e1e47420e41"
org_stores <- get_org_stores(org_uuid)
treez_stores <- filter(org_stores, main_pos == "treez")$facility
ll_stores <- filter(org_stores, main_pos == "leaflogix")$facility
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
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
ll_customers <- map_dfr(ll_stores, function(store) {
  customers <- db_read_table_unique(
    pg, paste(org, store, "ll_customers", sep = "_"), "customerId", "run_date_utc",
    columns = c(
      '"customerId"', '"firstName"', '"lastName"', '"postalCode"', '"emailAddress"',
      '"dateOfBirth"', '"customerType"', '"creationDate"', '"lastModifiedDateUTC"',
      "address1", "address2", '"discountGroups"', "phone", '"cellPhone"',
      "gender", "name", "city", "state", "run_date_utc"
    )
  )
}) |> distinct(customerId, .keep_all = TRUE)

ll_customers <- build_ll_customers(ll_customers, org) |> select(-loyalty_pts)
loyalty <- map_dfr(ll_stores, function(store) {
  dbGetQuery(pg, paste0(
    'select "customerId" as customer_id, "loyaltyBalance" as loyalty_pts from ',
    paste(org, store, "ll_loyalty", sep = "_"),
    " where run_date_utc = (select MAX(run_date_utc) FROM ",
    paste(org, store, "ll_loyalty", sep = "_"), ")"
  ))
}) |> distinct(customer_id, .keep_all = TRUE)
dbd(pg)

# Filling some missing data.
ll_customers$source_system <- "leaflogix"
ll_customers <- ll_customers |>
  left_join(loyalty, by = "customer_id") |>
  mutate(loyalty_pts = ifelse(is.na(loyalty_pts), 0, as.numeric(loyalty_pts)))


# Bind Treez and LLogix customers.
customers <- bind_rows(treez_customers, ll_customers)
customers$run_date_utc <- max(customers$run_date_utc, na.rm = TRUE)

# Write -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbCommit(pg)
dbd(pg)
