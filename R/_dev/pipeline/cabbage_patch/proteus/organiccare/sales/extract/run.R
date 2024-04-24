box::use(
  pipelinetools[rd_raw_archive, check_cols],
  hcaproteus[extract_proteus_sales],
  hcaconfig[dbc, dbd],
  DBI[dbWriteTable, dbListFields],
  lubridate[now],
  dplyr[mutate]
)

# vars --------------------------------------------------------------------
org <- "organiccare"
out_table <- paste(org, "proteus_invoices", sep = "_")
out_table_li <- paste(org, "proteus_invoice_line_items", sep = "_")

# read --------------------------------------------------------------------
js <- rd_raw_archive("sales", org, "proteus")

# query -------------------------------------------------------------------
invoices <- extract_proteus_sales(js)
now_utc <- now(tzone = "UTC")
line_items <- mutate(invoices$invoice_line_items, run_date_utc = now_utc)
invoices <- mutate(invoices$invoices, run_date_utc = now_utc)

# write -------------------------------------------------------------------
pg <- dbc("prod2", "cabbage_patch")
invoices <- check_cols(invoices, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, invoices, append = TRUE)
line_items <- check_cols(line_items, dbListFields(pg, out_table_li))
dbWriteTable(pg, out_table_li, line_items, append = TRUE)
dbd(pg)
