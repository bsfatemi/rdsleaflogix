# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org transitioned from one POS to another.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[bind_rows, coalesce, filter, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd, get_org_stores, orgTimeZone],
  hcaleaflogix[build_ll_orders],
  lubridate[now],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "happytime"
org_uuid <- "9ab20f4a-265d-4e7b-8464-4de209c5c693"
org_stores <- get_org_stores(org_uuid)
stores <- org_stores$facility
stores_name <- org_stores$full_name
stores_uuid <- org_stores$store_uuid
tz <- orgTimeZone(org_uuid)
out_table <- paste0(org, "_orders")

# Run -------------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
customers_mapping <- dbGetQuery(
  pg, "SELECT pos_customer_id, hca_customer_id FROM happytime_customers_mapping"
)
dbd(pg)

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
klicktrack_orders <- dbReadTable(pg, "happytime_klicktrack_orders")
customers <- db_read_table_unique(
  pg, paste0(org, "_customers"),
  c("klicktrack_customer_id", "customer_id"), "run_date_utc",
  c("klicktrack_customer_id", "customer_id")
)
dbd(pg)

# Match klicktrack customer IDs to the new IDs.
klicktrack_orders <- rename(klicktrack_orders, klicktrack_customer_id = customer_id) |>
  mutate(klicktrack_customer_id = paste0("klicktrack_", klicktrack_customer_id)) |>
  left_join(
    select(customers, klicktrack_customer_id, customer_id), by = "klicktrack_customer_id"
  ) |>
  select(-klicktrack_customer_id)

# Join.
orders <- bind_rows(ll_orders, klicktrack_orders)
orders$run_date_utc <- now("UTC")

# Fix orders' customer_ids, by using the `customers_mapping`.
orders <- left_join(orders, customers_mapping, by = c(customer_id = "pos_customer_id")) |>
  mutate(customer_id = coalesce(hca_customer_id, customer_id)) |>
  select(-hca_customer_id)

# Write -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbCommit(pg)
dbd(pg)
