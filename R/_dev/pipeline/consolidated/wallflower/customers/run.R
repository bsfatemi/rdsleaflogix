# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org has particular requests.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbWriteTable],
  dplyr[arrange, coalesce, distinct, filter, group_by, left_join, mutate, ungroup, row_number,
        select, transmute],
  hcaconfig[dbc, dbd, get_org_stores, lookupOrgGuid],
  hcaleaflogix[build_ll_customers],
  lubridate[as_datetime],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr],
  hcapipelines[plIndex]
)

# Vars ------------------------------------------------------------------------
org <- "wallflower"
stores <- get_org_stores(lookupOrgGuid(org))$facility
out_table <- plIndex()[short_name == org]$customers
out_table_cm <- "wallflower_customers_mapping"

# Run -------------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
customers <- map_dfr(stores, function(store) {
  customers <- db_read_table_unique(
    pg, paste(org, store, "ll_customers", sep = "_"), "customerId", "run_date_utc",
    columns = c(
      '"customerId"', '"firstName"', '"lastName"', '"postalCode"', '"emailAddress"',
      '"dateOfBirth"', '"customerType"', '"creationDate"', '"lastModifiedDateUTC"',
      "address1", "address2", '"discountGroups"', "phone", '"cellPhone"',
      "gender", "name", "city", "state", "run_date_utc"
    )
  )
}) |> distinct(customerId, .keep_all = TRUE)

customers <- build_ll_customers(customers, org) |> select(-loyalty_pts)
loyalty <- map_dfr(stores, function(store) {
  dbGetQuery(pg, paste0(
    'select "customerId" as customer_id, "loyaltyBalance" as loyalty_pts from ',
    paste(org, store, "ll_loyalty", sep = "_"),
    " where run_date_utc = (select MAX(run_date_utc) FROM ",
    paste(org, store, "ll_loyalty", sep = "_"), ")"
  ))
}) |> distinct(customer_id, .keep_all = TRUE)
dbd(pg)

# Filling some missing data.
customers$source_system <- "leaflogix"
customers <- customers |>
  left_join(loyalty, by = "customer_id") |>
  mutate(loyalty_pts = ifelse(is.na(loyalty_pts), 0, as.numeric(loyalty_pts)))

# For this org, we must use the phone number as the customer ID. So, let's create this customers
# mapping that maps each phone, with all the assigned customer IDs.
customers_mapping <- distinct(customers, customer_id, .keep_all = TRUE) |>
  transmute(
    pos_customer_id = customer_id,
    hca_customer_id = coalesce(phone, customer_id)
  )
# For each phone (used as customer ID), keep the last updated row metadata.
customers <- arrange(customers, desc(as_datetime(last_updated_utc))) |>
  mutate(customer_id = coalesce(phone, customer_id)) |>
  distinct(customer_id, .keep_all = TRUE)

# Write -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbExecute(pg, paste("TRUNCATE TABLE", out_table_cm))
dbWriteTable(pg, out_table_cm, customers_mapping, append = TRUE)
dbCommit(pg)
dbd(pg)
