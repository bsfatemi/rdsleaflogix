# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org has multiple POS in use sequentially (for different stores).

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  dplyr[bind_rows, filter, group_by, row_number, ungroup],
  hcablaze[build_blaze_customers],
  hcaconfig[dbc, dbd, get_org_stores],
  hcameadow[build_meadow_customers],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------
org <- "cookies"
org_uuid <- "04b4d115-a955-4033-81cd-fb9be423b618"
org_stores <- get_org_stores(org_uuid)
blaze_stores <- filter(org_stores, main_pos == "blaze")$facility
meadow_stores <- filter(org_stores, main_pos == "meadow")$facility
out_table <- paste0(org, "_customers")

# Run ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
blaze_customers <- map_dfr(blaze_stores, function(store) {
  members <- db_read_table_unique(
    pg, paste(org, store, "blaze_members", sep = "_"), "id", "run_date_utc"
  )
  build_blaze_customers(members, org, store)
})
meadow_customers <- map_dfr(meadow_stores, function(store) {
  customers <- db_read_table_unique(
    pg, paste(org, store, "meadow_customers", sep = "_"), "id", "run_date_utc"
  )
  build_meadow_customers(customers, org)
})
dbd(pg)

# Each customer can have a record per store, so building store by store cannot detect org-wide
# duplicates customer_id and created_at_utc chosen because they cannot be changed (unlike all other
# values)
blaze_customers <- group_by(blaze_customers, customer_id, created_at_utc) |>
  filter(row_number(last_updated_utc) == max(row_number(last_updated_utc))) |>
  ungroup()

# Bind Blaze and Meadow customers.
customers <- bind_rows(blaze_customers, meadow_customers)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbCommit(pg)
dbd(pg)
