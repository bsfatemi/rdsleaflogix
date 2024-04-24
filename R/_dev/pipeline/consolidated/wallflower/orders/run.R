# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org has particular requests.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbWriteTable],
  dplyr[coalesce, left_join, mutate, select],
  hcaconfig[dbc, dbd, orgTimeZone, get_org_stores, lookupOrgGuid],
  hcaleaflogix[build_ll_orders],
  pipelinetools[db_read_table_unique],
  hcapipelines[plIndex],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "wallflower"
org_uuid <- lookupOrgGuid(org)
org_stores <- get_org_stores(org_uuid)
stores <- org_stores$facility
stores_name <- org_stores$full_name
tz <- orgTimeZone(org_uuid)
out_table <- plIndex()[short_name == org]$orders

# READ ------------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
customers_mapping <- dbGetQuery(
  pg, "SELECT pos_customer_id, hca_customer_id FROM wallflower_customers_mapping"
)
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- map_dfr(seq_along(stores), function(i) {
  store <- stores[[i]]
  transactions <- db_read_table_unique(
    pg, paste(org, store, "ll_transactions", sep = "_"), "transactionId", "run_date_utc",
    columns = c(
      '"transactionId"', '"customerId"', '"transactionDate"', '"totalBeforeTax"',
      '"total"', "run_date_utc", '"isVoid"', '"transactionType"', '"employeeId"', '"totalDiscount"',
      '"orderSource"', '"tax"', '"isReturn"', '"transactionDateLocalTime"',
      '"terminalName"', '"revenueFeesAndDonations"'
    )
  )
  # customer request to use terminalname instead of orderType as order_type
  transactions$orderType <- transactions$terminalName
  employees <- db_read_table_unique(
    pg, paste(org, store, "ll_employees", sep = "_"), "userId", "run_date_utc",
    columns = c('"userId" AS sold_by_id', '"fullName" AS sold_by')
  )
  build_ll_orders(transactions, org, stores_name[[i]], tz, store) |>
    select(-sold_by) |>
    left_join(employees, by = "sold_by_id")
})
dbd(pg)

# Fix orders' customer_ids, by using the `customers_mapping`.
orders <- left_join(orders, customers_mapping, by = c(customer_id = "pos_customer_id")) |>
  mutate(customer_id = coalesce(hca_customer_id, customer_id)) |>
  select(-hca_customer_id)

# WRITE -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbCommit(pg)
dbd(pg)
