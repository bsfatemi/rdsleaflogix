# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcagreenbits[build_gb_customers],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------
org <- args$org_short_name
stores <- args$stores_short_names[[1]]
out_table <- args$customers

(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))

# Run ---------------------------------------------------------------------
pg <- dbc(db_cfg, "cabbage_patch")
customers <- map_dfr(stores, function(store) {
  db_read_table_unique(pg, paste(org, store, "gb_customers", sep = "_"), "id", "run_date_utc") |>
    build_gb_customers(org) |>
    mutate(customer_id = paste(store, customer_id, sep = "-"))
})
dbd(pg)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbCommit(pg)
dbd(pg)
