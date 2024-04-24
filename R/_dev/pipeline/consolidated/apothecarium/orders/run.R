# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

# Libraries ---------------------------------------------------------------
box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[bind_rows, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd, lookupOrgGuid, orgTimeZone],
  hcasweed[build_sweed_orders],
  lubridate[now],
  pipelinetools[db_read_table_unique]
)
# Vars --------------------------------------------------------------------
org <- "apothecarium"
org_uuid <- lookupOrgGuid(org)
tz <- orgTimeZone(org_uuid)

# Read --------------------------------------------------------------------
pg <- dbc("prod2", "consolidated")
# have more columns but only selected ones thats needed
akerna_orders <- dbReadTable(pg, "apothecarium_akerna_orders")
stores_info <- dbReadTable(pg, "apothecarium_hca_stores_info")
customers <- dbGetQuery(
  pg, "SELECT DISTINCT akerna_customer_id, customer_id FROM apothecarium_customers"
)
dbd(pg)

pg <- dbc("prod2", "cabbage_patch")
sweed_sales <- db_read_table_unique(
  pg, "apothecarium_sweed_sales", "InvoiceID", "run_date_utc"
)
sweed_sales_discounts <- db_read_table_unique(
  pg, "apothecarium_sweed_sales_discounts", "InvoiceID", "run_date_utc"
)
sweed_order_lines <- db_read_table_unique(
  pg, "apothecarium_sweed_sales_order_line_items", "InvoiceItemID", "run_date_utc"
)
sweed_facilities <- db_read_table_unique(
  pg, "apothecarium_sweed_facilities", "FacilityId", "run_date_utc"
)
dbd(pg)

# Run ---------------------------------------------------------------------
# For Sweed ------
sweed_orders <- build_sweed_orders(
  sweed_sales, sweed_sales_discounts, sweed_order_lines, sweed_facilities, stores_info, org, tz
)
akerna_orders <- rename(akerna_orders, akerna_customer_id = customer_id) |>
  mutate(
    akerna_customer_id = paste0("akerna_", akerna_customer_id),
    source_run_date_utc = as.character(source_run_date_utc)
  ) |>
  left_join(customers, by = "akerna_customer_id") |>
  select(-akerna_customer_id)
# Discard Akerna extra columns.
akerna_orders <- akerna_orders[, colnames(sweed_orders)]
# Join --------------------------------------------------------------------
orders <- bind_rows(sweed_orders, akerna_orders)
orders$order_facility <- toupper(orders$order_facility)
orders$run_date_utc <- now("UTC")

# Write -------------------------------------------------------------------
con <- dbc("prod2", "consolidated")
dbBegin(con)
dbExecute(con, paste("TRUNCATE TABLE", "apothecarium_orders"))
dbWriteTable(con, "apothecarium_orders", orders, append = TRUE)
dbCommit(con)
dbd(con)
