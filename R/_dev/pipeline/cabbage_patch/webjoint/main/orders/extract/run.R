box::use(
  DBI[dbWriteTable, dbListFields],
  dplyr[mutate, filter],
  hcaconfig[dbc, dbd],
  hcawebjoint[extract_orders],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "wj_orders", sep = "_")
out_table_od <- paste(org, store, "wj_orders_details", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("orders", paste(org, store, sep = "_"), "webjoint")

# EXTRACT -----------------------------------------------------------------
orders <- extract_orders(json)
now_utc <- now(tzone = "UTC")

orders_details <- mutate(orders$orders_details, run_date_utc = now_utc)
orders <- mutate(orders$orders, run_date_utc = now_utc)
# since WJ API is bad we are pulling in too much duplicaitive data this is to  limit that.
orders <- orders |> filter(status == "Complete")
orders_details <- orders_details |> filter(order_id %in% orders$id)
# LAND --------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- check_cols(orders, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
orders_details <- check_cols(orders_details, dbListFields(pg, out_table_od))
dbWriteTable(pg, out_table_od, orders_details, append = TRUE)
dbd(pg)
