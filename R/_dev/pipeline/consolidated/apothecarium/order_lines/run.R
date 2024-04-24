# Consolidated Order Lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.
# Libraries ---------------------------------------------------------------
box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[bind_rows, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd],
  hcasweed[build_sweed_order_lines],
  lubridate[now],
  pipelinetools[db_read_table_unique, get_categories_mapping]
)

# VARS --------------------------------------------------------------------
org <- "apothecarium"
org_uuid <- "b6c6ad09-62dc-43a5-9f88-c3c6f5a53eda"
stores <- c("berkeley", "marina", "soma", "capitola", "castro")

# READ FROM DB-CB ---------------------------------------------------------
pg <- dbc("prod2", "cabbage_patch")
sweed_order_line_items <- db_read_table_unique(
  pg, "apothecarium_sweed_sales_order_line_items", "InvoiceItemID", "run_date_utc"
)
sweed_sales <- db_read_table_unique(
  pg, "apothecarium_sweed_sales", "InvoiceID", "run_date_utc"
)
sweed_products <- db_read_table_unique(
  pg, "apothecarium_sweed_products", "ProductId", "UpdatedAtUTC"
)
dbd(pg)

cats_map <- get_categories_mapping(org_uuid)
pg <- dbc("prod2", "consolidated")
class_map <- dbReadTable(pg, "product_classes")
customers <- dbGetQuery(
  pg, "SELECT DISTINCT akerna_customer_id, customer_id FROM apothecarium_customers"
)
akerna_order_lines <- dbReadTable(pg, "apothecarium_akerna_order_lines")
dbd(pg)

# Run ---------------------------------------------------------------------
# For Sweed ------
sweed_order_lines <- build_sweed_order_lines(
  sweed_order_line_items, sweed_sales, sweed_products, cats_map, class_map, org
)


# Match Akerna customer IDs to the new IDs.
akerna_order_lines <- rename(akerna_order_lines, akerna_customer_id = customer_id) |>
  mutate(akerna_customer_id = paste0("akerna_", akerna_customer_id)) |>
  left_join(customers, by = "akerna_customer_id") |>
  select(-akerna_customer_id)
# Discard Akerna extra columns.
akerna_order_lines <- akerna_order_lines[, colnames(sweed_order_lines)]

# Join --------------------------------------------------------------------
order_lines <- bind_rows(sweed_order_lines, akerna_order_lines)
order_lines$run_date_utc <- now("UTC")

# WRITE -------------------------------------------------------------------
con <- dbc("prod2", "consolidated")
dbBegin(con)
dbExecute(con, paste("TRUNCATE TABLE", "apothecarium_order_lines"))
dbWriteTable(con, "apothecarium_order_lines", order_lines, append = TRUE)
dbCommit(con)
dbd(con)
