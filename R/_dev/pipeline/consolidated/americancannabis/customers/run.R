# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.
box::use(
  hcaconfig[dbc, dbd],
  hcaproteus[build_proteus_customers],
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  pipelinetools[db_read_table_unique]
)

# Run ---------------------------------------------------------------------
org <- "americancannabis"

# Connect and Read In Data
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
customers <- db_read_table_unique(
  pg, paste0(org, "_proteus_customers"), "customer_id", "run_date_utc"
)
customer_types <- db_read_table_unique(
  pg, paste0(org, "_proteus_invoices"), "customer_id", "dt_created",
  columns = c("customer_id", "customer_type")
)
dbd(pg)

# Build -------------------------------------------------------------------
customers <- build_proteus_customers(org, customers, customer_types)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", paste0(org, "_customers")))
dbWriteTable(pg, paste0(org, "_customers"), customers, append = TRUE)
dbCommit(pg)
dbd(pg)
