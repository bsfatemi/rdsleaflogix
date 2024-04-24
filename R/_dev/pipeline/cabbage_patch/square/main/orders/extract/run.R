box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcasquare[extract_orders],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "square_orders", sep = "_")
out_table_li <- paste(org, store, "square_orders_items", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("orders", paste(org, store, sep = "_"), "square")

# EXTRACT -----------------------------------------------------------------
orders <- extract_orders(json)
now_utc <- now("UTC")
line_items <- mutate(orders$line_items, run_date_utc = now_utc)
orders <- mutate(orders$orders, run_date_utc = now_utc)

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- check_cols(orders, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
line_items <- check_cols(line_items, dbListFields(pg, out_table_li))
dbWriteTable(pg, out_table_li, line_items, append = TRUE)
dbd(pg)
