box::use(
  DBI[dbAppendTable, dbBegin, dbCommit, dbExecute],
  dplyr[arrange, bind_rows, collect, desc, distinct, filter, group_by, mutate, tbl, union],
  hcaconfig[dbc, dbd, lookupOrgGuid, orgAuth],
  hcapipelines[plAppDataIndex],
  lubridate[as_datetime, days, today],
  polarispub[subset_blocked_by_carrier],
  purrr[map_dfr]
)

### Vars.
(query_date <- today() - days(1))
(oldest_data <- today() - days(30))

### Query.

# Query new data.
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
sent_tel_sms <- tbl(pg, "tel_sms") |>
  filter(
    !is.na(to_1_carrier), nchar(to_1_carrier) > 0,
    as_date(received_at) == query_date,
    direction == "outbound"
  ) |>
  distinct(phone = to_1_phone_number, carrier = to_1_carrier, updated_at_utc = received_at)
sent_sms_logs <- tbl(pg, "mstudio_sms_logs") |>
  filter(
    !is.na(data_to_1_carrier), nchar(data_to_1_carrier) > 0,
    as_date(data_received_at) == query_date,
    data_direction == "outbound"
  ) |>
  distinct(
    phone = data_to_1_phone_number, carrier = data_to_1_carrier, updated_at_utc = data_received_at
  )
sent_sms <- union(sent_tel_sms, sent_sms_logs) |>
  mutate(updated_at_utc = as_datetime(updated_at_utc)) |>
  group_by(phone) |>
  filter(updated_at_utc == max(updated_at_utc, na.rm = TRUE)) |>
  collect()
dbd(pg)

# Query old data, that is still valid according to `oldest_data`.
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "integrated")
phones_carriers <- tbl(pg, "phones_carriers") |>
  collect()
old_lookups <- filter(phones_carriers, updated_at_utc < oldest_data)
phones_carriers <- filter(phones_carriers, updated_at_utc >= oldest_data)

### Transform.
# Join data, and keep the last updated data (per phone).
phones_carriers <- bind_rows(sent_sms, phones_carriers) |>
  arrange(desc(updated_at_utc)) |>
  distinct(phone, .keep_all = TRUE)

### Push.
dbBegin(pg)
dbExecute(pg, "TRUNCATE phones_carriers")
dbAppendTable(pg, "phones_carriers", phones_carriers)
dbCommit(pg)
dbd(pg)

### Get active Polaris orgs phone numbers.
active_orgs <- plAppDataIndex()[
  polaris_is_active == TRUE & current_client == TRUE, unique(short_name)
]
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "appdata")
orgs_phones <- map_dfr(active_orgs, function(org) {
  orgs_phones <- tbl(pg, "polaris_appdata") |>
    filter(org == !!org) |>
    distinct(last_customer_phone) |>
    collect()
}) |>
  distinct(last_customer_phone)
dbd(pg)

# Update phone carriers for phones numbers that we consider we have old carrier data.
# `subset_blocked_by_carrier` lookups and updates the `phones_carriers` table.
setdiff(old_lookups$phone, orgs_phones$last_customer_phone) |>
  setdiff(phones_carriers$phone) |>
  subset_blocked_by_carrier(creds = orgAuth(lookupOrgGuid("demoorg"), "telnyx"))
