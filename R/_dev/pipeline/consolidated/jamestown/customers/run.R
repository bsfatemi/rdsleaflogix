# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

# Source ------------------------------------------------------------------
source("inst/pipeline/consolidated/jamestown/customers/init.R", local = TRUE)
source(
  "inst/pipeline/consolidated/jamestown/customers/src/build_jamestown_trz2_customers.R",
  local = TRUE
)

# Read ---------------------------------------------------------------------
org <- "jamestown"
# Connect and Read In Data
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
df <- pipelinetools::db_read_table_unique(
  pg, "jamestown_main_trz2_customers", "customer_id", "run_date_utc"
)
best <- pipelinetools::db_read_table_unique(
  pg, "jamestown_mesa_trz2_customers", "customer_id", "run_date_utc"
)
hcaconfig::dbd(pg)

# Run ---------------------------------------------------------------------
james <- df |> hcatreez::build_trz2_customers(org)
james$customer_id <- paste0("yuma-", james$customer_id)

best <- df |> hcatreez::build_trz2_customers(org)
best$customer_id <- paste0("mesa-", best$customer_id)

customers <- dplyr::bind_rows(james, best)

# Write -------------------------------------------------------------------
pg <- hcaconfig::dbc("prod2", "consolidated")
DBI::dbBegin(pg)
DBI::dbExecute(pg, paste("TRUNCATE TABLE", "jamestown_customers"))
DBI::dbWriteTable(pg, "jamestown_customers", customers, append = TRUE)
DBI::dbCommit(pg)
hcaconfig::dbd(pg)
