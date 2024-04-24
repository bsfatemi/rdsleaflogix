box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcagrowflow[extract_orders],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "growflow_orders", sep = "_")
out_table_oi <- paste(org, store, "growflow_order_items", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("orders", paste(org, store, sep = "_"), "growflow")

# EXTRACT -----------------------------------------------------------------
orders <- extract_orders(json)
now_utc <- now("UTC")
order_items <- mutate(orders$order_items, run_date_utc = now_utc)
orders <- mutate(orders$orders, run_date_utc = now_utc)

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- check_cols(orders, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
order_items <- check_cols(order_items, dbListFields(pg, out_table_oi))
dbWriteTable(pg, out_table_oi, order_items, append = TRUE)
dbd(pg)
