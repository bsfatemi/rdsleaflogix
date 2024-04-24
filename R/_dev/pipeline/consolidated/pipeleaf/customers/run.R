# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd, get_org_stores, lookupOrgGuid],
  hcaflowhub[build_flowhub_customers],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr],
  hcapipelines[plIndex]
)

# Vars ---------------------------------------------------------------------
org <- "pipeleaf"
stores <- get_org_stores(lookupOrgGuid(org))$facility
out_table <- plIndex()[short_name == org]$customers

# Read ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
customers <- map_dfr(stores, function(store) {
  db_read_table_unique(
    pg, paste(org, store, "flowhub_customers", sep = "_"), "id", "updated_at"
  ) |>
    build_flowhub_customers(org) |>
    mutate(customer_id = paste0(store, "-", customer_id))
})
dbd(pg)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbCommit(pg)
dbd(pg)
