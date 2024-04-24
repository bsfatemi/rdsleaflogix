# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

# Vars ------------------------------------------------------------------
org <- "hightimes"
stores <- c(
  "redding", "shastalake", "oakland", "coalinga", "maywood", "sacramento", "broadway", "blythe",
  "sanbernardino"
)
out_table <- paste0(org, "_customers")

# Run ---------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
customers <- purrr::map_dfr(stores, function(store) {
  members <- pipelinetools::db_read_table_unique(
    pg, paste(org, store, "blaze_members", sep = "_"), "id", "run_date_utc"
  )
  hcablaze::build_blaze_customers(members, org, store)
})
hcaconfig::dbd(pg)

# Each customer can have a record per store, so building store by store cannot detect org-wide
# duplicates customer_id and created_at_utc chosen because they cannot be changed (unlike all other
# values)
customers <- dplyr::group_by(customers, customer_id, created_at_utc) |>
  dplyr::filter(dplyr::row_number(last_updated_utc) == max(dplyr::row_number(last_updated_utc))) |>
  dplyr::ungroup()

# Write -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
DBI::dbBegin(pg)
DBI::dbExecute(pg, paste("TRUNCATE TABLE", out_table))
DBI::dbWriteTable(pg, out_table, customers, append = TRUE)
DBI::dbCommit(pg)
hcaconfig::dbd(pg)
