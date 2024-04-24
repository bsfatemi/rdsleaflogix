# Consolidated Order Lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  hcaconfig[dbc, dbd, get_org_stores, lookupOrgGuid],
  hcasquare[build_square_order_lines],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  purrr[map_dfr],
  hcapipelines[plIndex]
)

# Vars ------------------------------------------------------------------
org <- "blomshop"
org_uuid <- lookupOrgGuid(org)
stores <- get_org_stores(org_uuid)$facility
out_table <- plIndex()[short_name == org]$order_lines
# Run ---------------------------------------------------------------------
cats_map <- get_categories_mapping(org_uuid)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
order_lines <- map_dfr(stores, function(store) {
  orders <- db_read_table_unique(
    pg, paste(org, store, "square_orders", sep = "_"), "id", "run_date_utc",
    columns = c("id", "run_date_utc", "customer_id", "state", "total_tip_money", "tenders")
  )
  orders_items <- db_read_table_unique(
    pg, paste(org, store, "square_orders_items", sep = "_"), c("order_id", "uid"), "run_date_utc"
  )
  catalog_items <- db_read_table_unique(
    pg, paste(org, store, "square_catalog_items", sep = "_"), "id", "run_date_utc",
    columns = c("id", "run_date_utc", "category_id", "variations")
  )
  catalog_item_variations <- db_read_table_unique(
    pg, paste(org, store, "square_catalog_item_variations", sep = "_"), "id", "run_date_utc",
    columns = c("id", "run_date_utc", "sku")
  )
  catalog_categories <- db_read_table_unique(
    pg, paste(org, store, "square_catalog_categories", sep = "_"), "id", "run_date_utc",
    columns = c("id", "run_date_utc", "name")
  )
  build_square_order_lines(
    orders, orders_items, catalog_items, catalog_item_variations, catalog_categories, cats_map, org
  )
})
dbd(pg)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
