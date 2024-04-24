# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbExistsTable, dbGetQuery, dbWriteTable],
  dplyr[distinct, filter],
  hcaconfig[dbc, dbd],
  hcaleaflogix[build_ll_orders],
  logger[log_info],
  lubridate[days, now, today],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- args$org_short_name
stores <- args$stores_short_names[[1]]
stores_name <- args$stores_full_names[[1]]
tz <- args$tz
update_day <- format(today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1)))
out_table <- args$orders

# READ ------------------------------------------------------------------------
log_info("Reading in employees tables for org: ", org)
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
employees <- map_dfr(stores, function(store) {
  dbGetQuery(pg, paste0(
    'select "userId" as sold_by_id, "fullName" as sold_by from ',
    paste(org, store, "ll_employees", sep = "_"),
    " where run_date_utc = (select MAX(run_date_utc) FROM ",
    paste(org, store, "ll_employees", sep = "_"), ")"
  ))
}) |> distinct(sold_by_id = as.character(sold_by_id), .keep_all = TRUE)

log_info("Building orders for org: ", org)
orders <- map_dfr(seq_along(stores), function(i) {
  store <- stores[[i]]
  log_info("Building orders for store: ", store)
  transactions <- db_read_table_unique(
    pg, paste(org, store, "ll_transactions", sep = "_"), "transactionId", "run_date_utc",
    columns = c(
      '"transactionId"', '"customerId"', '"transactionDate"', '"totalBeforeTax"',
      '"total"', "run_date_utc", '"isVoid"', '"transactionType"', '"employeeId"', '"totalDiscount"',
      '"orderSource"', '"tax"', '"isReturn"', '"transactionDateLocalTime"',
      '"orderType"', '"revenueFeesAndDonations"'
    ),
    extra_filters = paste0("run_date_utc >= '", update_day, "'")
  )
  if (nrow(transactions) == 0) {
    return(NULL)
  }
  build_ll_orders(transactions, org, stores_name[[i]], tz, store)
})
dbd(pg)

# WRITE -----------------------------------------------------------------------
if (nrow(orders) > 0) {
  orders$run_date_utc <- now("UTC")
  employee_out <- paste(org, "employees_temp", sep = "_")
  pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
  dbWriteTable(
    pg, paste(org, "orders_temp", sep = "_"), distinct(orders, order_id),
    temporary = TRUE
  )
  dbWriteTable(pg, employee_out, employees, temporary = TRUE)
  dbBegin(pg)
  log_info("Deleting order rows that have had an update since last run. (refunds, etc.)")
  if (dbExistsTable(pg, out_table)) {
    dbExecute(pg, paste0(
      'DELETE FROM "', out_table, '" WHERE order_id IN (SELECT order_id FROM "',
      paste(org, "orders_temp", sep = "_"), '")'
    ))
  }
  log_info("Writing consolidated orders to database table.")
  dbWriteTable(pg, out_table, orders, append = TRUE)
  log_info("Updating employee information since last run")
  dbExecute(pg, paste0(
    "UPDATE ", out_table, " SET sold_by = ", employee_out, ".sold_by FROM ",
    employee_out, " WHERE ", out_table, ".sold_by_id = ", employee_out,
    ".sold_by_id AND (", out_table, ".sold_by != ", employee_out,
    ".sold_by OR ", out_table, ".sold_by IS NULL)"
  ))
  dbCommit(pg)
  dbd(pg)
}
log_info("Consolidated orders script finished for org: ", org)
