box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcaleaflogix[extract_transactions],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "ll_transactions", sep = "_")
out_table_li <- paste(org, store, "ll_line_items", sep = "_")
out_table_lid <- paste(org, store, "ll_line_items_discounts", sep = "_")
out_table_lit <- paste(org, store, "ll_line_items_taxes", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("transactions", paste(org, store, sep = "_"), "leaflogix")

# EXTRACT -----------------------------------------------------------------
now_utc <- now(tzone = "UTC")
transactions <- extract_transactions(json)
line_items <- mutate(transactions$line_items, run_date_utc = !!now_utc)
line_items_discounts <- mutate(transactions$line_items_discounts, run_date_utc = !!now_utc)
line_items_taxes <- mutate(transactions$line_items_taxes, run_date_utc = !!now_utc)
transactions <- mutate(transactions$transactions, run_date_utc = !!now_utc)

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
transactions <- check_cols(transactions, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, transactions, append = TRUE)
line_items <- check_cols(line_items, dbListFields(pg, out_table_li))
dbWriteTable(pg, out_table_li, line_items, append = TRUE)
line_items_discounts <- check_cols(line_items_discounts, dbListFields(pg, out_table_lid))
dbWriteTable(pg, out_table_lid, line_items_discounts, append = TRUE)
line_items_taxes <- check_cols(line_items_taxes, dbListFields(pg, out_table_lit))
dbWriteTable(pg, out_table_lit, line_items_taxes, append = TRUE)
dbd(pg)
