# VARS --------------------------------------------------------------------
org <- args$org
out_table <- paste(org, "magento_orders", sep = "_")
out_table_oi <- paste(org, "magento_order_items", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("orders", paste(org, sep = "_"), "magento")

# EXTRACT -----------------------------------------------------------------
now_utc <- lubridate::now(tzone = "UTC")
orders <- hcamagento::extract_orders(json)
order_items <- orders$order_items

orders <- orders$orders
orders$run_date_utc <- now_utc
order_items$run_date_utc <- now_utc

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- pipelinetools::check_cols(orders, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, orders, append = TRUE)
order_items <- pipelinetools::check_cols(order_items, DBI::dbListFields(pg, out_table_oi))
DBI::dbWriteTable(pg, out_table_oi, order_items, append = TRUE)
hcaconfig::dbd(pg)
