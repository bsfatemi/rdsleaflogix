# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

###### CUSTOM CODE: because this org transitioned from one POS to another.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[
    anti_join, arrange, bind_rows, coalesce, count, desc, distinct, filter, group_by, left_join,
    mutate, pull, ungroup, row_number, select
  ],
  hcaconfig[dbc, dbd, get_org_stores],
  hcaleaflogix[build_ll_customers],
  lubridate[as_datetime, now],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------------
org <- "diem"
org_uuid <- "fda29e34-f03e-4a03-975b-d1febb1b7406"
org_stores <- get_org_stores(org_uuid)
out_table <- paste0(org, "_customers")

# Run -------------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
flowhub_customers <- dbReadTable(pg, "diem_flowhub_customers")
blaze_customers <- dbReadTable(pg, "diem_blaze_customers")
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
# Discard Blaze/FlowHub phone numbers that are repeated in multiple LL customers.
ll_multiple_phone_customers <- count(ll_customers, phone) |>
  filter(n > 1) |>
  pull(phone)
# LL doesn't return opt-ins, so keep their old data opt-ins.
ll_customers <- select(ll_customers, -pos_is_subscribed)
# For each phone number, keep their last updated opt status.
optins <- bind_rows(
  select(flowhub_customers, phone, pos_is_subscribed, last_updated_utc),
  select(blaze_customers, phone, pos_is_subscribed, last_updated_utc)
) |>
  arrange(desc(as_datetime(last_updated_utc))) |>
  distinct(phone, .keep_all = TRUE) |>
  select(phone, pos_is_subscribed) |>
  filter(pos_is_subscribed)
# We will merge by `phone`, so remove duplicates by `last_updated_utc`.
blaze_customers <- arrange(blaze_customers, desc(last_updated_utc)) |>
  filter(!is.na(phone), !phone %in% ll_multiple_phone_customers) |>
  distinct(phone, .keep_all = TRUE) |>
  mutate(
    customer_id = paste0("blaze_", customer_id),
    blaze_customer_id = customer_id
  )
flowhub_customers <- arrange(flowhub_customers, desc(last_updated_utc)) |>
  filter(!is.na(phone), !phone %in% ll_multiple_phone_customers) |>
  distinct(phone, .keep_all = TRUE) |>
  mutate(
    customer_id = paste0("flowhub_", customer_id),
    flowhub_customer_id = customer_id
  )
# For each joined customer in LL, give their Blaze/FlowHub `customer_id`.
customers <- left_join(
  ll_customers, select(blaze_customers, blaze_customer_id, phone),
  by = "phone"
) |>
  left_join(select(flowhub_customers, flowhub_customer_id, phone), by = "phone") |>
  left_join(optins, by = "phone") |>
  mutate(pos_is_subscribed = coalesce(pos_is_subscribed, FALSE))
# Bind Blaze/FlowHub customers that couldn't be joined by phone number.
customers <- bind_rows(
  customers,
  filter(blaze_customers, !phone %in% customers$phone),
  filter(flowhub_customers, !phone %in% c(customers$phone, blaze_customers$phone))
)
customers$run_date_utc <- now("UTC")

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbCommit(pg)
dbd(pg)
