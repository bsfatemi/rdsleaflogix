# Consolidated Order Lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[bind_rows, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd, lookupOrgGuid, get_org_stores],
  hcameadow[build_meadow_order_lines],
  hcapipelines[plIndex],
  lubridate[now],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------
org <- "socalgreenbuddy"
org_uuid <- lookupOrgGuid(org)
stores <- get_org_stores(lookupOrgGuid(org))$facility
out_table <- plIndex()[short_name == org]$order_lines

# Run ---------------------------------------------------------------------
cats_map <- get_categories_mapping(org_uuid)
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
class_map <- dbReadTable(pg, "product_classes")
customers <- dbGetQuery(
  pg, paste0("SELECT DISTINCT io_customer_id, customer_id FROM ", org, "_customers")
)
io_order_lines <- dbReadTable(pg, "socalgreenbuddy_io_order_lines")
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
meadow_order_lines <- map_dfr(stores, function(store) {
  orders <- db_read_table_unique(
    pg, paste(org, store, "meadow_orders", sep = "_"), "id", "run_date_utc"
  )
  line_items <- db_read_table_unique(
    pg, paste(org, store, "meadow_line_items", sep = "_"), "order_line_id", "run_date_utc"
  )
  products <- db_read_table_unique(
    pg, paste(org, store, "meadow_products", sep = "_"), "id", "run_date_utc"
  )
  build_meadow_order_lines(orders, line_items, products, cats_map, class_map, org)
})
dbd(pg)

# Match io customer IDs to the new IDs.
io_order_lines <- rename(io_order_lines, io_customer_id = customer_id) |>
  mutate(io_customer_id = paste0("io_", io_customer_id)) |>
  left_join(customers, by = "io_customer_id") |>
  select(-io_customer_id)

# Join ------------------------------------------------------------------------
order_lines <- bind_rows(meadow_order_lines, io_order_lines)
order_lines$run_date_utc <- now("UTC")

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
