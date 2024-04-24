# Consolidated Order Lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcagrowflow[build_growflow_order_lines],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------
org <- args$org_short_name
org_uuid <- args$org_uuid
stores <- args$stores_short_names[[1]]
out_table <- args$order_lines

# Run ---------------------------------------------------------------------
cats_map <- get_categories_mapping(org_uuid)
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
class_map <- dbReadTable(pg, "product_classes")
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
order_lines <- map_dfr(stores, function(store) {
  orders <- db_read_table_unique(
    pg, paste(org, store, "growflow_orders", sep = "_"), "id", "run_date_utc"
  )
  order_items <- db_read_table_unique(
    pg, paste(org, store, "growflow_order_items", sep = "_"), c("order_id", "id"), "run_date_utc"
  )
  build_growflow_order_lines(orders, order_items, cats_map, class_map, org)
})
dbd(pg)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
