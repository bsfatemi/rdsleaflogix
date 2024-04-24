# Consolidated Order Lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org transitioned from one POS to another.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[bind_rows, coalesce, distinct, filter, group_by, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd, get_org_stores],
  hcaleaflogix[build_ll_order_lines, build_ll_products],
  lubridate[now],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "happytime"
org_uuid <- "9ab20f4a-265d-4e7b-8464-4de209c5c693"
stores <- get_org_stores(org_uuid)$facility
out_table <- paste0(org, "_order_lines")

# Run -------------------------------------------------------------------------
cats_map <- get_categories_mapping(org_uuid)
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
class_map <- dbReadTable(pg, "product_classes")
customers <- db_read_table_unique(
  pg, paste0(org, "_customers"),
  c("klicktrack_customer_id", "customer_id"), "run_date_utc",
  c("klicktrack_customer_id", "customer_id")
)
klicktrack_order_lines <- dbReadTable(pg, "happytime_klicktrack_order_lines")
customers_mapping <- dbGetQuery(
  pg, "SELECT pos_customer_id, hca_customer_id FROM happytime_customers_mapping"
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

ll_order_lines <- map_dfr(stores, function(store) {
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

# Match klicktrack customer IDs to the new IDs.
klicktrack_order_lines <- rename(klicktrack_order_lines, klicktrack_customer_id = customer_id) |>
  mutate(klicktrack_customer_id = paste0("klicktrack_", klicktrack_customer_id)) |>
  left_join(
    select(customers, klicktrack_customer_id, customer_id), by = "klicktrack_customer_id"
  ) |>
  select(-klicktrack_customer_id)

# Join.
order_lines <- bind_rows(ll_order_lines, klicktrack_order_lines)
order_lines$run_date_utc <- now("UTC")

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
