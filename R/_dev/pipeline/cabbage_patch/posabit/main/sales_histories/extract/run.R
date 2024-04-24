box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcaposabit[extract_sales_histories],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "posabit_orders", sep = "_")
out_table_oi <- paste(org, store, "posabit_order_items", sep = "_")

# READ --------------------------------------------------------------------
json_files <- rd_raw_archive("sales_histories", paste(org, store, sep = "_"), "posabit")

# EXTRACT -----------------------------------------------------------------
sales_histories <- extract_sales_histories(json_files)
now_utc <- now("UTC")
order_items <- mutate(sales_histories$order_items, run_date_utc = now_utc)
orders <- mutate(sales_histories$orders, run_date_utc = now_utc)

# LAND --------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- check_cols(orders, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
order_items <- check_cols(order_items, dbListFields(pg, out_table_oi))
dbWriteTable(pg, out_table_oi, order_items, append = TRUE)
dbd(pg)
