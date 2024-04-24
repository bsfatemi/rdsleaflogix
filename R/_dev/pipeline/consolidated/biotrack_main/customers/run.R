# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcabiotrack[build_biotrack_customers],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------
org <- args$org_short_name
stores <- args$stores_short_names[[1]]
out_table <- args$customers

# Run ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
customers <- map_dfr(stores, function(store) {
  customers <- db_read_table_unique(
    pg, paste(org, store, "biotrack_customers", sep = "_"), "customerid", "run_date_utc",
    columns = c(
      "address1", "address2", "birthday", "birthmonth", "birthyear", "cell", "city", "created",
      "customerid", "deleted", "email", "firstname", "ismember", "lastname", "licensenum",
      "licensetext", "modified", "phone", "points", "run_date_utc", "sex", "smsoptin", "state",
      "zip"
    )
  )
  build_biotrack_customers(customers, org)
})
dbd(pg)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbCommit(pg)
dbd(pg)
