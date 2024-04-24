# Consolidated Order Lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org transitioned from one POS to another.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  dplyr[bind_rows, distinct, filter, group_by, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd, get_org_stores],
  hcaleaflogix[build_ll_order_lines, build_ll_products],
  lubridate[now],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "diem"
org_uuid <- "fda29e34-f03e-4a03-975b-d1febb1b7406"
stores <- get_org_stores(org_uuid)$facility
out_table <- paste0(org, "_order_lines")

# Run -------------------------------------------------------------------------
cats_map <- get_categories_mapping(org_uuid)
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
class_map <- dbReadTable(pg, "product_classes")
customers <- db_read_table_unique(
  pg, paste0(org, "_customers"),
  c("flowhub_customer_id", "blaze_customer_id", "customer_id"), "run_date_utc",
  c("flowhub_customer_id", "blaze_customer_id", "customer_id")
)
flowhub_order_lines <- dbReadTable(pg, "diem_flowhub_order_lines")
blaze_order_lines <- dbReadTable(pg, "diem_blaze_order_lines")
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
# Match FlowHub customer IDs to the new IDs.
flowhub_order_lines <- rename(flowhub_order_lines, flowhub_customer_id = customer_id) |>
  mutate(flowhub_customer_id = paste0("flowhub_", flowhub_customer_id)) |>
  left_join(select(customers, flowhub_customer_id, customer_id), by = "flowhub_customer_id") |>
  select(-flowhub_customer_id)
# Match Blaze customer IDs to the new IDs.
blaze_order_lines <- rename(blaze_order_lines, blaze_customer_id = customer_id) |>
  mutate(blaze_customer_id = paste0("blaze_", blaze_customer_id)) |>
  left_join(select(customers, blaze_customer_id, customer_id), by = "blaze_customer_id") |>
  select(-blaze_customer_id)

# Join.
order_lines <- bind_rows(ll_order_lines, blaze_order_lines, flowhub_order_lines)
order_lines$run_date_utc <- now("UTC")

# WRITE -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
