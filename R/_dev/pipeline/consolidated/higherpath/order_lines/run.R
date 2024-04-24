# Consolidated Order Lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org has multiple POS in use sequentially (for different stores).

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  dplyr[bind_rows, filter, mutate],
  hcaconfig[dbc, dbd, get_org_stores],
  hcasquare[build_square_order_lines],
  hcatreez[build_trz2_order_lines],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "higherpath"
org_uuid <- "174ba423-593e-4431-9e4d-a912cdac3362"
org_stores <- get_org_stores(org_uuid)
treez_stores <- filter(org_stores, main_pos == "treez")$facility
square_stores <- filter(org_stores, main_pos == "square")$facility
out_table <- paste0(org, "_order_lines")

# Run ---------------------------------------------------------------------
cats_map <- get_categories_mapping(org_uuid)
pg <- dbc("prod2", "consolidated")
class_map <- dbReadTable(pg, "product_classes")
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
treez_order_lines <- map_dfr(treez_stores, function(store) {
  tickets <- db_read_table_unique(
    pg, paste(org, store, "trz2_tickets", sep = "_"), "order_number", "run_date_utc",
    columns = c("order_number", "run_date_utc", "customer_id", "ticket_id")
  )
  ticket_items <- db_read_table_unique(
    pg, paste(org, store, "trz2_ticket_items", sep = "_"), "ticket_id", "run_date_utc",
    with_ties = TRUE,
    columns = c("ticket_id", "product_id", "run_date_utc", "quantity", "price_sell", "price_total")
  )
  ticket_discounts <- db_read_table_unique(
    pg, paste(org, store, "trz2_ticket_discounts", sep = "_"), "ticket_id", "run_date_utc",
    with_ties = TRUE, columns = c("ticket_id", "run_date_utc", "product_id", "savings")
  )
  products <- db_read_table_unique(
    pg, paste(org, store, "trz2_products", sep = "_"), "product_id", "last_updated_at",
    columns = c(
      "product_id", "last_updated_at", "name", "product_barcodes", "category_type",
      "classification", "brand", "amount", "uom"
    )
  )
  build_trz2_order_lines(
    tickets, ticket_items, ticket_discounts, products, cats_map, class_map, org
  ) |>
    mutate(customer_id = paste0(store, "-", customer_id))
})
square_order_lines <- map_dfr(square_stores, function(store) {
  orders <- db_read_table_unique(
    pg, paste(org, store, "square_orders", sep = "_"), "id", "run_date_utc",
    columns = c("id", "run_date_utc", "customer_id", "state", "total_tip_money", "tenders")
  )
  orders_items <- db_read_table_unique(
    pg, paste(org, store, "square_orders_items", sep = "_"), c("order_id", "uid"), "run_date_utc",
    columns = c(
      "order_id", "uid", "run_date_utc", "catalog_object_id", "name", "quantity",
      "total_money_amount", "total_discount_money_amount", "total_tax_money_amount",
      "gross_sales_money_amount"
    )
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

# Bind Treez and Square order_lines.
order_lines <- bind_rows(treez_order_lines, square_order_lines)
order_lines$run_date_utc <- max(order_lines$run_date_utc, na.rm = TRUE)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
