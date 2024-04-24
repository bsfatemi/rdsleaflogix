# Consolidated order_lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org has multiple POS in use sequentially (for different stores).

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  dplyr[bind_rows, filter],
  hcablaze[build_blaze_order_lines],
  hcaconfig[dbc, dbd, get_org_stores],
  hcameadow[build_meadow_order_lines],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------
org <- "cookies"
org_uuid <- "04b4d115-a955-4033-81cd-fb9be423b618"
org_stores <- get_org_stores(org_uuid)
blaze_stores <- filter(org_stores, main_pos == "blaze")
meadow_stores <- filter(org_stores, main_pos == "meadow")
out_table <- paste0(org, "_order_lines")

# Run ---------------------------------------------------------------------
cats_map <- get_categories_mapping(org_uuid)
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
class_map <- dbReadTable(pg, "product_classes")
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
blaze_order_lines <- map_dfr(blaze_stores$facility, function(store) {
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
meadow_order_lines <- map_dfr(meadow_stores$facility, function(store) {
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

# Bind Blaze and Meadow order_lines.
order_lines <- bind_rows(blaze_order_lines, meadow_order_lines)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
