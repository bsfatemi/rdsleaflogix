# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbExistsTable, dbGetQuery, dbWriteTable],
  dplyr[arrange, desc, distinct, filter, group_by, ungroup, row_number],
  hcaconfig[dbc, dbd],
  hcaleaflogix[build_ll_customers],
  logger[log_info],
  lubridate[days, now, today],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- args$org_short_name
stores <- args$stores_short_names[[1]]
out_table <- args$customers
update_day <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))

# Run -------------------------------------------------------------------------
log_info("Starting consolidated customers table build for org: ", org)
# Each customer can have a record per store, so building store by store cannot detect org-wide
# so grabbing all cabbage patch tables and getting distinct values before doing build_ll_customers
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
customers <- map_dfr(stores, function(store) {
  log_info("Reading cabbage_patch customers table for store: ", store)
  customers <- db_read_table_unique(
    pg, paste(org, store, "ll_customers", sep = "_"), "customerId", "run_date_utc",
    columns = c(
      '"customerId"', '"firstName"', '"lastName"', '"postalCode"', '"emailAddress"',
      '"dateOfBirth"', '"customerType"', '"creationDate"', '"lastModifiedDateUTC"',
      "address1", "address2", '"discountGroups"', "phone", '"cellPhone"',
      "gender", "name", "city", "state", "run_date_utc"
    ),
    extra_filters = paste0("run_date_utc >= '", update_day, "'")
  )
}) |> distinct(customerId, .keep_all = TRUE)

customers <- build_ll_customers(customers, org)
log_info("Consolidated customers table finished building for org: ", org)

loyalty <- map_dfr(stores, function(store) {
  log_info("Reading cabbage_patch loyalty table for store: ", store)
  dbGetQuery(pg, paste0(
    'select "customerId" as customer_id, "loyaltyBalance" as loyalty_pts from ',
    paste(org, store, "ll_loyalty", sep = "_"),
    " where run_date_utc = (select MAX(run_date_utc) FROM ",
    paste(org, store, "ll_loyalty", sep = "_"), ")"
  ))
}) |> distinct(customer_id = as.character(customer_id), .keep_all = TRUE)
dbd(pg)

# Write -----------------------------------------------------------------------
if (nrow(customers) > 0) {
  customers$run_date_utc <- now("UTC")
  loyalty_out <- paste(org, "loyalty_temp", sep = "_")
  pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
  dbWriteTable(
    pg, paste(org, "customers_temp", sep = "_"), distinct(customers, customer_id),
    temporary = TRUE
  )
  dbWriteTable(pg, loyalty_out, loyalty, temporary = TRUE)
  log_info("Updating customer records if there are updates.")
  dbBegin(pg)
  if (dbExistsTable(pg, out_table)) {
    dbExecute(pg, paste0(
      'DELETE FROM "', out_table, '" WHERE customer_id IN (SELECT customer_id FROM "',
      paste(org, "customers_temp", sep = "_"), '")'
    ))
  }
  dbWriteTable(pg, out_table, customers, append = TRUE)
  log_info("Consolidated customers table written to database.")

  log_info("Updating customer loyalty_pts if there are updates.")
  dbExecute(pg, paste0(
    "UPDATE ", out_table, " SET loyalty_pts = ",
    loyalty_out, ".loyalty_pts FROM ",
    loyalty_out, " WHERE ", out_table, ".customer_id = ", loyalty_out,
    ".customer_id AND ", out_table, ".loyalty_pts != ", loyalty_out,
    ".loyalty_pts"
  ))
  dbCommit(pg)
  dbd(pg)
}
log_info("Consolidated customers script for org: ", org)
