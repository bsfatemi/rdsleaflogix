# Consolidated Onfleet Deliveries
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

# Vars ------------------------------------------------------------------
org <- args$short_name
out_table <- paste(org, "deliveries", sep = "_")

# Run ---------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
tasks <- pipelinetools::db_read_table_unique(
  pg, paste(org, "onfleet_tasks", sep = "_"), "id", "time_last_modified"
)
workers <- pipelinetools::db_read_table_unique(
  pg, paste(org, "onfleet_workers", sep = "_"), "id", "time_last_modified"
)
organizations <- pipelinetools::db_read_table_unique(
  pg, paste(org, "onfleet_organizations", sep = "_"), "id", "time_last_modified"
)
hcaconfig::dbd(pg)

# Build -------------------------------------------------------------------
deliveries <- hcaonfleet::build_onfleet_deliveries(tasks, workers, organizations, org)

# Write -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
DBI::dbBegin(pg)
DBI::dbExecute(pg, paste("TRUNCATE TABLE", out_table))
DBI::dbWriteTable(pg, out_table, deliveries, append = TRUE)
DBI::dbCommit(pg)
hcaconfig::dbd(pg)
