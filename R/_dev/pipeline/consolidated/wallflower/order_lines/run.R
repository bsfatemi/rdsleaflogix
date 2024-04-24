# Consolidated Order Lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org has particular requests.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[coalesce, distinct, filter, group_by, left_join, mutate, select],
  hcaconfig[dbc, dbd, get_org_stores, lookupOrgGuid],
  hcaleaflogix[build_ll_order_lines, build_ll_products],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  hcapipelines[plIndex],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "wallflower"
org_uuid <- lookupOrgGuid(org)
stores <- get_org_stores(org_uuid)$facility
out_table <- plIndex()[short_name == org]$order_lines

# Run -------------------------------------------------------------------------
cats_map <- get_categories_mapping(org_uuid)
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
class_map <- dbReadTable(pg, "product_classes")
customers_mapping <- dbGetQuery(
  pg, "SELECT pos_customer_id, hca_customer_id FROM wallflower_customers_mapping"
)
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
products <- map_dfr(stores, function(store) {
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

order_lines <- map_dfr(stores, function(store) {
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
order_lines <- order_lines |> left_join(products, by = "product_id")

# Fix order_lines' customer_ids, by using the `customers_mapping`.
order_lines <- left_join(order_lines, customers_mapping, by = c(customer_id = "pos_customer_id")) |>
  mutate(customer_id = coalesce(hca_customer_id, customer_id)) |>
  select(-hca_customer_id)

# WRITE -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
