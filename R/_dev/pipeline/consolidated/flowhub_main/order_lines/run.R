# Consolidated order_lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  dplyr[mutate, select],
  hcaconfig[dbc, dbd],
  hcaflowhub[build_flowhub_order_lines],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  purrr[map_dfr]
)

# Vars ---------------------------------------------------------------------
org <- args$org_short_name
org_uuid <- args$org_uuid
stores <- args$stores_short_names[[1]]
out_table <- args$order_lines

# Run ---------------------------------------------------------------------
cats_map <- select(get_categories_mapping(org_uuid), -run_date_utc)
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
class_map <- select(dbReadTable(pg, "product_classes"), -run_date_utc)
dbd(pg)

# Read ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
order_lines <- map_dfr(stores, function(store) {
  orders <- db_read_table_unique(
    pg, paste(org, store, "flowhub_orders", sep = "_"), "id", "run_date_utc"
  )
  order_items <- db_read_table_unique(
    pg, paste(org, store, "flowhub_orders_items_in_cart", sep = "_"), c("id", "id_2"),
    "run_date_utc"
  )
  products <- db_read_table_unique(
    pg, paste(org, store, "flowhub_products", sep = "_"), c("product_id", "sku", "product_name"),
    "run_date_utc"
  )
  build_flowhub_order_lines(orders, order_items, products, cats_map, class_map, org) |>
    mutate(customer_id = paste0(store, "-", customer_id))
})
dbd(pg)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
