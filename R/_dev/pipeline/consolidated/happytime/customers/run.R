# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org transitioned from one POS to another.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[
    anti_join, arrange, bind_rows, coalesce, count, desc, distinct, filter, group_by, left_join,
    mutate, pull, row_number, select, transmute, ungroup
  ],
  hcaconfig[dbc, dbd, get_org_stores],
  hcaleaflogix[build_ll_customers],
  lubridate[as_datetime, now],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "happytime"
org_uuid <- "9ab20f4a-265d-4e7b-8464-4de209c5c693"
org_stores <- get_org_stores(org_uuid)
out_table <- paste0(org, "_customers")
out_table_cm <- "happytime_customers_mapping"
# Run -------------------------------------------------------------------------

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
klicktrack_customers <- dbReadTable(pg, "happytime_klicktrack_customers")
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
ll_customers <- map_dfr(org_stores$facility, function(store) {
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

ll_customers <- build_ll_customers(ll_customers, org) |> select(-loyalty_pts)
loyalty <- map_dfr(org_stores$facility, function(store) {
  dbGetQuery(pg, paste0(
    'select "customerId" as customer_id, "loyaltyBalance" as loyalty_pts from ',
    paste(org, store, "ll_loyalty", sep = "_"),
    " where run_date_utc = (select MAX(run_date_utc) FROM ",
    paste(org, store, "ll_loyalty", sep = "_"), ")"
  ))
}) |> distinct(customer_id, .keep_all = TRUE)
dbd(pg)

# Filling some missing data.
ll_customers$source_system <- "leaflogix"
ll_customers <- ll_customers |>
  left_join(loyalty, by = "customer_id") |>
  mutate(loyalty_pts = ifelse(is.na(loyalty_pts), 0, as.numeric(loyalty_pts)))

# Discard klicktrack phone numbers that are repeated in multiple LL customers.
ll_multiple_phone_customers <- count(ll_customers, phone) |>
  filter(n > 1) |>
  pull(phone)
# We will merge by `phone`, so remove duplicates by `last_updated_utc`.
klicktrack_customers <- arrange(klicktrack_customers, desc(last_updated_utc)) |>
  filter(!is.na(phone), !phone %in% ll_multiple_phone_customers) |>
  distinct(phone, .keep_all = TRUE) |>
  mutate(
    customer_id = paste0("klicktrack_", customer_id),
    klicktrack_customer_id = customer_id
  )
# For each joined customer in klicktrack, give their ll `customer_id`.
customers <- left_join(
  ll_customers, select(klicktrack_customers, klicktrack_customer_id, phone),
  by = "phone"
)
# Bind ll customers that couldn't be joined by phone number.
klicktrack_customers$source_run_date_utc <- as.character(klicktrack_customers$source_run_date_utc)
customers <- bind_rows(
  customers, anti_join(klicktrack_customers, customers, by = "phone")
)
customers$run_date_utc <- now("UTC")


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

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbExecute(pg, paste("TRUNCATE TABLE", out_table_cm))
dbWriteTable(pg, out_table_cm, customers_mapping, append = TRUE)
dbCommit(pg)
dbd(pg)
