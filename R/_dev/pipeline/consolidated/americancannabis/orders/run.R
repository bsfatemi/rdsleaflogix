# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.
box::use(
  hcaconfig[dbc, dbd],
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  hcaproteus[build_proteus_orders],
  pipelinetools[db_read_table_unique]
)

# Run ---------------------------------------------------------------------
org <- "americancannabis"
order_facility <- "Main"
facility <- "main"

pg <- dbc("prod2", "cabbage_patch")
invoices <- db_read_table_unique(
  pg, "americancannabis_proteus_invoices", "invoice_id", "run_date_utc"
)
dbd(pg)

# Build.
orders <- build_proteus_orders(invoices, org, facility, order_facility)

# Write -------------------------------------------------------------------
pg <- dbc("prod2", "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", "americancannabis_orders"))
dbWriteTable(pg, "americancannabis_orders", orders, append = TRUE)
dbCommit(pg)
dbd(pg)
