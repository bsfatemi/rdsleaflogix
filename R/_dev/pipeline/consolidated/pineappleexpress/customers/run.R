# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  dplyr[
    anti_join, arrange, bind_rows, count, desc, distinct, filter, left_join, mutate, pull, select
  ],
  hcablaze[build_blaze_customers],
  hcaconfig[dbc, dbd, get_org_stores, lookupOrgGuid],
  hcapipelines[plIndex],
  lubridate[now],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "pineappleexpress"
stores <- get_org_stores(lookupOrgGuid(org))$facility
out_table <- plIndex()[short_name == org]$customers
# Run -------------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
blaze_customers <- map_dfr(stores, function(store) {
  members <- db_read_table_unique(
    pg, paste(org, store, "blaze_members", sep = "_"), "id", "run_date_utc"
  )
  build_blaze_customers(members, org, store)
})
dbd(pg)
# Each customer can have a record per store, so building store by store cannot detect org-wide
# duplicates customer_id and created_at_utc chosen because they cannot be changed (unlike all other
# values)
blaze_customers <- dplyr::group_by(blaze_customers, customer_id, created_at_utc) |>
  dplyr::filter(dplyr::row_number(last_updated_utc) == max(dplyr::row_number(last_updated_utc))) |>
  dplyr::ungroup()


pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
treez_customers <- dbReadTable(pg, "pineappleexpress_treez_customers")
dbd(pg)

# Join --------------------------------------------------------------------
# Discard Treez phone numbers that are repeated in multiple Blaze customers.
blaze_multiple_phone_customers <- count(blaze_customers, phone) |>
  filter(n > 1) |>
  pull(phone)
# We will merge by `phone`, so remove duplicates, by `last_updated_utc`.
treez_customers <- arrange(treez_customers, desc(last_updated_utc)) |>
  filter(!is.na(phone), !phone %in% blaze_multiple_phone_customers) |>
  distinct(phone, .keep_all = TRUE) |>
  mutate(
    customer_id = paste0("treez_", customer_id),
    treez_customer_id = customer_id
  )
# For each joined customer in Blaze, give their Treez `customer_id`.
customers <- left_join(
  blaze_customers, select(treez_customers, treez_customer_id, phone),
  by = "phone"
)
# Bind Treez customers that couldn't be joined by phone number.
customers <- bind_rows(customers, anti_join(treez_customers, customers, by = "phone"))
customers$run_date_utc <- now("UTC")

# Write -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", paste(org, "customers", sep = "_")))
dbWriteTable(pg, paste(org, "customers", sep = "_"), customers, append = TRUE)
dbCommit(pg)
dbd(pg)
