box::use(
  DBI[dbListFields, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcagreenbits[extract_gb_orders],
  lubridate[now],
  pipelinetools[rd_raw_archive, check_cols]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "gb_orders", sep = "_")
out_table_li <- paste(org, store, "gb_order_line_items", sep = "_")
out_table_tax <- paste(org, store, "gb_order_line_items_tax_breakdowns", sep = "_")
out_table_disc <- paste(org, store, "gb_order_line_items_discounts", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("orders", paste(org, store, sep = "_"), "greenbits")

# EXTRACT -----------------------------------------------------------------
orders <- extract_gb_orders(json)
run_utc <- now(tzone = "UTC")
line_items <- orders$order_line_items
discounts <- orders$order_line_items_discounts
taxes <- orders$order_line_items_tax_breakdowns
orders <- orders$orders

line_items$run_date_utc <- run_utc
discounts$run_date_utc <- run_utc
taxes$run_date_utc <- run_utc
orders$run_date_utc <- run_utc

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- check_cols(orders, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)

line_items <- check_cols(line_items, dbListFields(pg, out_table_li))
dbWriteTable(pg, out_table_li, line_items, append = TRUE)

discounts <- check_cols(discounts, dbListFields(pg, out_table_disc))
dbWriteTable(pg, out_table_disc, discounts, append = TRUE)

taxes <- check_cols(taxes, dbListFields(pg, out_table_tax))
dbWriteTable(pg, out_table_tax, taxes, append = TRUE)
dbd(pg)
