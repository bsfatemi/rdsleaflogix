# VARS --------------------------------------------------------------------
org <- args$org
out_table <- paste(org, "sweed_sales", sep = "_")
out_table_oli <- paste(org, "sweed_sales_order_line_items", sep = "_")
out_table_d <- paste(org, "sweed_sales_discounts", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("data", org, "sweed")

# EXTRACT -----------------------------------------------------------------
sales <- hcasweed::extract_sales(json)
run_utc <- lubridate::now(tzone = "UTC")
orders <- sales$orders
order_line_items <- sales$order_line_items
discounts <- sales$discounts
orders$run_date_utc <- run_utc
order_line_items$run_date_utc <- run_utc
discounts$run_date_utc <- run_utc

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- pipelinetools::check_cols(orders, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, orders, append = TRUE)
order_line_items <- pipelinetools::check_cols(
  order_line_items, DBI::dbListFields(pg, out_table_oli)
)
DBI::dbWriteTable(pg, out_table_oli, order_line_items, append = TRUE)
discounts <- pipelinetools::check_cols(discounts, DBI::dbListFields(pg, out_table_d))
DBI::dbWriteTable(pg, out_table_d, discounts, append = TRUE)
hcaconfig::dbd(pg)
