box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcalightspeed[extract_sales],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "ls_sales", sep = "_")
out_table_si <- paste(org, store, "ls_sale_lines", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("sales", paste(org, store, sep = "_"), "lightspeed")

# EXTRACT -----------------------------------------------------------------
sales <- extract_sales(json)
now_utc <- now("UTC")
sale_lines <- mutate(sales$sale_lines, run_date_utc = now_utc)
sales <- mutate(sales$sales, run_date_utc = now_utc)

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
sales <- check_cols(sales, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, sales, append = TRUE)
sale_lines <- check_cols(sale_lines, dbListFields(pg, out_table_si))
dbWriteTable(pg, out_table_si, sale_lines, append = TRUE)
dbd(pg)
