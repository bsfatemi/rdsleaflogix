# Consolidated Order Lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org has multiple POS in use sequentially (for different stores).

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  dplyr[bind_rows, distinct, filter, group_by, left_join, mutate, select],
  hcaconfig[dbc, dbd, get_org_stores],
  hcaleaflogix[build_ll_order_lines, build_ll_products],
  hcatreez[build_trz2_order_lines],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "verano"
org_uuid <- "a6cefdc6-0561-48ee-88cf-7e1e47420e41"
org_stores <- get_org_stores(org_uuid)
treez_stores <- filter(org_stores, main_pos == "treez")$facility
ll_stores <- filter(org_stores, main_pos == "leaflogix")$facility
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

products <- map_dfr(ll_stores, function(store) {
  products <- db_read_table_unique(
    pg, paste(org, store, "ll_products", sep = "_"), "productId", "run_date_utc",
    columns = c(
      '"productId"', "sku", '"productName"', '"brandId"', '"brandName"', "size",
      '"strainType"', "category", "run_date_utc", '"lastModifiedDateUTC"'
    )
  )
  build_ll_products(products, cats_map, class_map)
}) |>
  group_by(product_id) |>
  filter(lastModifiedDateUTC == max(lastModifiedDateUTC)) |>
  distinct(product_id, .keep_all = TRUE) |>
  select(-lastModifiedDateUTC)

ll_order_lines <- map_dfr(ll_stores, function(store) {
  line_items <- db_read_table_unique(
    pg, paste(org, store, "ll_line_items", sep = "_"), "transactionItemId", "run_date_utc"
  )
  products <- db_read_table_unique(
    pg, paste(org, store, "ll_products", sep = "_"), "productId", "run_date_utc"
  )
  transactions <- db_read_table_unique(
    pg, paste(org, store, "ll_transactions", sep = "_"), "transactionId", "run_date_utc"
  )
  taxes <- db_read_table_unique(
    pg, paste(org, store, "ll_line_items_taxes", sep = "_"), c("transactionItemId", "rateName"),
    "run_date_utc"
  )
  build_ll_order_lines(
    line_items, transactions, taxes, org
  )
})
dbd(pg)
ll_order_lines <- ll_order_lines |> left_join(products, by = "product_id")

# Bind Treez and LLogix order_lines.
order_lines <- bind_rows(treez_order_lines, ll_order_lines)
order_lines$run_date_utc <- max(order_lines$run_date_utc, na.rm = TRUE)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
