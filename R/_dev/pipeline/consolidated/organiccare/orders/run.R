# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.
box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcaproteus[build_proteus_orders],
  pipelinetools[db_read_table_unique]
)

# Run ---------------------------------------------------------------------
org <- "organiccare"
order_facility <- "Organic Care of California"
facility <- "main"

pg <- dbc("prod2", "cabbage_patch")
invoices <- db_read_table_unique(pg, "organiccare_proteus_invoices", "invoice_id", "run_date_utc")
dbd(pg)

# Build.
orders <- build_proteus_orders(invoices, org, facility, order_facility, delivery_only = TRUE)

# Write -------------------------------------------------------------------
pg <- dbc("prod2", "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", "organiccare_orders"))
dbWriteTable(pg, "organiccare_orders", orders, append = TRUE)
dbCommit(pg)
dbd(pg)
