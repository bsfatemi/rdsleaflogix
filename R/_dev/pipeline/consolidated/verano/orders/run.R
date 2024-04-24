# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org has multiple POS in use sequentially (for different stores).

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  dplyr[bind_rows, filter, left_join, mutate, select],
  hcaconfig[dbc, dbd, get_org_stores, orgTimeZone],
  hcaleaflogix[build_ll_orders],
  hcatreez[build_trz2_orders],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "verano"
org_uuid <- "a6cefdc6-0561-48ee-88cf-7e1e47420e41"
org_stores <- get_org_stores(org_uuid)
treez_stores <- filter(org_stores, main_pos == "treez")
ll_stores <- filter(org_stores, main_pos == "leaflogix")
tz <- orgTimeZone(org_uuid)
out_table <- paste0(org, "_orders")

# Run -------------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
treez_orders <- map_dfr(seq_len(nrow(treez_stores)), function(i) {
  store <- treez_stores[i, ]
  db_read_table_unique(
    pg, paste(org, store$facility, "trz2_tickets", sep = "_"), "order_number", "run_date_utc"
  ) |>
    build_trz2_orders(org, store$full_name, store$facility, tz, store$store_uuid) |>
    mutate(customer_id = paste0(store$facility, "-", customer_id))
})
ll_orders <- map_dfr(seq_len(nrow(ll_stores)), function(i) {
  store <- ll_stores[i, ]
  transactions <- db_read_table_unique(
    pg, paste(org, store$facility, "ll_transactions", sep = "_"), "transactionId", "run_date_utc",
    columns = c(
      '"transactionId"', '"customerId"', '"transactionDate"', '"totalBeforeTax"',
      '"total"', "run_date_utc", '"isVoid"', '"transactionType"', '"employeeId"', '"totalDiscount"',
      '"orderSource"', '"tax"', '"isReturn"', '"transactionDateLocalTime"',
      '"orderType"', '"revenueFeesAndDonations"'
    )
  )
  employees <- db_read_table_unique(
    pg, paste(org, store$facility, "ll_employees", sep = "_"), "userId", "run_date_utc",
    columns = c('"userId" AS sold_by_id', '"fullName" AS sold_by')
  )
  build_ll_orders(transactions, org, store$full_name, tz, store$facility) |>
    select(-sold_by) |>
    left_join(employees, by = "sold_by_id")
})
dbd(pg)

# Bind Treez and LLogix orders.
orders <- bind_rows(treez_orders, ll_orders)
orders$run_date_utc <- max(orders$run_date_utc, na.rm = TRUE)

# Write -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbCommit(pg)
dbd(pg)
