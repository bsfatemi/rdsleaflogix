# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org transitioned from one POS to another.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  dplyr[bind_rows, filter, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd, get_org_stores, orgTimeZone],
  hcaflowhub[build_flowhub_orders],
  hcaleaflogix[build_ll_orders],
  lubridate[now],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "diem_ma"
org_uuid <- "5b9ee9de-45b4-4f44-86e1-ce2718fc0f95"
org_stores <- get_org_stores(org_uuid)
stores <- org_stores$facility
stores_name <- org_stores$full_name
stores_uuid <- org_stores$store_uuid
tz <- orgTimeZone(org_uuid)
out_table <- paste0(org, "_orders")

# Run -------------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
ll_orders <- map_dfr(seq_len(nrow(org_stores)), function(i) {
  store <- org_stores[i, ]
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

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
flowhub_orders <- dbReadTable(pg, "diem_ma_flowhub_orders")
customers <- db_read_table_unique(
  pg, paste0(org, "_customers"),
  c("flowhub_customer_id", "customer_id"), "run_date_utc",
  c("flowhub_customer_id", "customer_id")
)
dbd(pg)

# Match FlowHub customer IDs to the new IDs.
flowhub_orders <- rename(flowhub_orders, flowhub_customer_id = customer_id) |>
  mutate(flowhub_customer_id = paste0("flowhub_", flowhub_customer_id)) |>
  left_join(select(customers, flowhub_customer_id, customer_id), by = "flowhub_customer_id") |>
  select(-flowhub_customer_id)

# Join.
orders <- bind_rows(ll_orders, flowhub_orders)
orders$run_date_utc <- now("UTC")

# Write -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbCommit(pg)
dbd(pg)
