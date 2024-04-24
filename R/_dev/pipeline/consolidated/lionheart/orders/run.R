# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org transitioned from one POS to another.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  dplyr[bind_rows, filter, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd, get_org_stores, orgTimeZone],
  hcaleaflogix[build_ll_orders],
  lubridate[now],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "lionheart"
org_uuid <- "04cf5be8-5855-44ef-a40e-98145517dfd8"
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
akerna_orders <- dbReadTable(pg, "lionheart_akerna_orders")
customers <- db_read_table_unique(
  pg, paste0(org, "_customers"),
  c("akerna_customer_id", "customer_id"), "run_date_utc",
  c("akerna_customer_id", "customer_id")
)
dbd(pg)

# Match akerna customer IDs to the new IDs.
akerna_orders <- rename(akerna_orders, akerna_customer_id = customer_id) |>
  mutate(akerna_customer_id = paste0("akerna_", akerna_customer_id)) |>
  left_join(select(customers, akerna_customer_id, customer_id), by = "akerna_customer_id") |>
  select(-akerna_customer_id)

# Join.
orders <- bind_rows(ll_orders, akerna_orders)
orders$run_date_utc <- now("UTC")

# Write -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbCommit(pg)
dbd(pg)
