# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  hcablaze[build_blaze_customers],
  hcaconfig[dbc, dbd],
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
  members <- db_read_table_unique(
    pg, paste(org, store, "blaze_members", sep = "_"), "id", "run_date_utc"
  )
  build_blaze_customers(members, org, store)
})
dbd(pg)

# Each customer can have a record per store, so building store by store cannot detect org-wide
# duplicates customer_id and created_at_utc chosen because they cannot be changed (unlike all other
# values)
customers <- dplyr::group_by(customers, customer_id, created_at_utc) |>
  dplyr::filter(dplyr::row_number(last_updated_utc) == max(dplyr::row_number(last_updated_utc))) |>
  dplyr::ungroup()

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbCommit(pg)
dbd(pg)
