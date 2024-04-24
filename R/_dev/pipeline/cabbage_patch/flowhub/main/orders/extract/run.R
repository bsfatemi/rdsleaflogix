box::use(
  DBI[dbListFields, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcaflowhub[extract_fh_orders],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "flowhub_orders", sep = "_")
out_table_oi <- paste(org, store, "flowhub_orders_items_in_cart", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("orders", paste(org, store, sep = "_"), "flowhub")

# EXTRACT -----------------------------------------------------------------
orders <- extract_fh_orders(json)
orders_items_in_cart <- orders$orders_items_in_cart
orders <- orders$orders
orders$run_date_utc <- now("UTC")
orders_items_in_cart$run_date_utc <- now("UTC")

# STORE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- check_cols(orders, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
orders_items_in_cart <- check_cols(orders_items_in_cart, dbListFields(pg, out_table_oi))
dbWriteTable(pg, out_table_oi, orders_items_in_cart, append = TRUE)
dbd(pg)
