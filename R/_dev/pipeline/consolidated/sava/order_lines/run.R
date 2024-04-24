# Consolidated order_lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[bind_rows, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd, lookupOrgGuid],
  hcamagento[build_magento_order_lines],
  lubridate[now],
  pipelinetools[db_read_table_unique],
  stringr[str_glue],
)

# Vars ------------------------------------------------------------------
org <- "sava"
orguuid <- "3ce33391-3a34-46b5-8598-a2a6b4999089"
store <- "main"

# Run ---------------------------------------------------------------------
cats_map <- pipelinetools::get_categories_mapping(orguuid)
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
class_map <- dbReadTable(pg, "product_classes")
customers <- dbGetQuery(pg, str_glue(
  "SELECT DISTINCT blz_customer_id, customer_id FROM {org}_customers"
))
blz_order_lines <- dbReadTable(pg, "sava_blz_order_lines")
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
magento_order_items <- dbReadTable(pg, paste(org, "magento_order_items", sep = "_"))
magento_orders <- db_read_table_unique(
  pg, paste(org, "magento_orders", sep = "_"), "order_id", "run_date_utc"
)
magento_products <- db_read_table_unique(
  pg, paste(org, "magento_products", sep = "_"), "id", "run_date_utc"
)
magento_categories <- db_read_table_unique(
  pg, paste(org, "magento_categories", sep = "_"), "id", "run_date_utc"
)
magento_brands <- db_read_table_unique(
  pg, paste(org, "magento_brands", sep = "_"), "id", "run_date_utc"
)
dbd(pg)

# Run ---------------------------------------------------------------------
# For Blaze ------
# Match Blaze customer IDs to the new IDs.
blz_order_lines <- rename(blz_order_lines, blz_customer_id = customer_id) |>
  mutate(blz_customer_id = paste0("blz_", blz_customer_id)) |>
  left_join(customers, by = "blz_customer_id") |>
  select(-blz_customer_id)

# For Magento ------
magento_order_lines <- build_magento_order_lines(
  magento_order_items, magento_orders, magento_products, magento_categories, magento_brands,
  cats_map, class_map, org, store, orguuid
)

# Join --------------------------------------------------------------------
order_lines <- bind_rows(magento_order_lines, blz_order_lines)
order_lines$run_date_utc <- now("UTC")

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", paste0(org, "_order_lines")))
dbWriteTable(pg, paste0(org, "_order_lines"), order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
