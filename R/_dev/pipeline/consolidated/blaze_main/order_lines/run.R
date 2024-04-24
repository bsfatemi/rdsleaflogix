# Consolidated order_lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  hcablaze[build_blaze_order_lines],
  hcaconfig[dbc, dbd],
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
order_lines <- map_dfr(seq_along(stores), function(i) {
  store <- stores[[i]]
  cart_items <- db_read_table_unique(
    pg, paste(org, store, "blaze_transactions_cart_items", sep = "_"), c("transaction_id", "id"),
    "run_date_utc", columns = c(
      "id", "transaction_id", "run_date_utc", "quantity", "calc_discount", "final_price",
      "calc_tax", "cost", "product_id"
    )
  )
  transactions <- db_read_table_unique(
    pg, paste(org, store, "blaze_transactions", sep = "_"), "id", "run_date_utc", columns = c(
      "id", "member_id", "status", "cart_total_discount", "cart_total_calc_tax", "cart_total_fees",
      "run_date_utc"
    )
  )
  products <- db_read_table_unique(
    pg, paste(org, store, "blaze_products", sep = "_"), "id", "run_date_utc", columns = c(
      "id", "run_date_utc", "brand_id", "name", "weight_per_unit", "unit_value", "category_name",
      "flower_type", "sku"
    )
  )
  brands <- db_read_table_unique(
    pg, paste(org, store, "blaze_brands", sep = "_"), "id", "run_date_utc",
    columns = c("id", "run_date_utc", "name")
  )
  build_blaze_order_lines(
    cart_items, transactions, products, brands, cats_map, class_map, org, store, org_uuid
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
