# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

# Vars --------------------------------------------------------------------
org <- "jamestown"
orguuid <- hcaconfig::lookupOrgGuid(org)
tz <- hcaconfig::orgTimeZone(orguuid)

# Run ---------------------------------------------------------------------
# Connect and Read In Data
pg <- hcaconfig::dbc(cfg = "prod2", "cabbage_patch")
james <- pipelinetools::db_read_table_unique(
  pg, "jamestown_main_trz2_tickets", "ticket_id", "run_date_utc"
)
best <- pipelinetools::db_read_table_unique(
  pg, "jamestown_mesa_trz2_tickets", "ticket_id", "run_date_utc"
)
hcaconfig::dbd(pg)

james$customer_id <- paste0("yuma-", james$customer_id)
best$customer_id <- paste0("mesa-", best$customer_id)

james <- hcatreez::build_trz2_orders(james, org, "YUMA DISPENSARY", "main", tz)
james$facility <- "yuma"
best <- hcatreez::build_trz2_orders(best, org, "BEST DISPENSARY", "mesa", tz)
orders <- dplyr::bind_rows(james, best)

orders$run_date_utc <- lubridate::now(tzone = "UTC")
orders$org <- org

# Write -------------------------------------------------------------------
con <- hcaconfig::dbc("prod2", "consolidated")
DBI::dbBegin(con)
DBI::dbExecute(con, paste("TRUNCATE TABLE", "jamestown_orders"))
DBI::dbWriteTable(con, "jamestown_orders", orders, append = TRUE)
DBI::dbCommit(con)
hcaconfig::dbd(con)
