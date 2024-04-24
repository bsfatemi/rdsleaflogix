box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcacova[extract_invoices],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "cova_invoices", sep = "_")
out_table_li <- paste(org, store, "cova_line_items", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("invoices", paste(org, store, sep = "_"), "cova")

# EXTRACT -----------------------------------------------------------------
invoices <- extract_invoices(json)
now_utc <- now("UTC")
line_items <- mutate(invoices$line_items, run_date_utc = !!now_utc)
invoices <- mutate(invoices$invoices, run_date_utc = !!now_utc)

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
invoices <- check_cols(invoices, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, invoices, append = TRUE)
line_items <- check_cols(line_items, dbListFields(pg, out_table_li))
dbWriteTable(pg, out_table_li, line_items, append = TRUE)
dbd(pg)
