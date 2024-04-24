# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

org <- "hightimes"
stores <- c(
  "Coalinga" = "coalinga", "Sacramento" = "sacramento", "Shasta Lake" = "shastalake",
  "Redding" = "redding", "Oakland" = "oakland", "Broadway" = "broadway", "Maywood" = "maywood",
  "San Bernardino" = "sanbernardino", "Blythe" = "blythe"
)
out_table <- paste(org, "orders", sep = "_")
# Read ---------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- purrr::map_dfr(names(stores), function(store_name) {
  store <- stores[[store_name]]
  transactions <- pipelinetools::db_read_table_unique(
    pg, paste(org, store, "blaze_transactions", sep = "_"), "id", "run_date_utc"
  )
  employees <- pipelinetools::db_read_table_unique(
    pg, paste(org, store, "blaze_employees", sep = "_"), "id", "run_date_utc"
  )
  hcablaze::build_blaze_orders(transactions, employees, org, store = store_name, facility = store)
})
hcaconfig::dbd(pg)

# Write -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
DBI::dbBegin(pg)
DBI::dbExecute(pg, paste("TRUNCATE TABLE", out_table))
DBI::dbWriteTable(pg, out_table, orders, append = TRUE)
DBI::dbCommit(pg)
hcaconfig::dbd(pg)
