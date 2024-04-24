# Polaris Phones Info
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbAppendTable, dbBegin, dbCommit, dbExecute],
  dplyr[
    arrange, collect, desc, distinct, filter, full_join, left_join, mutate, select, tbl, transmute
  ],
  hcaconfig[dbc, dbd, orgIndex],
  hcapipelines[plAppDataIndex],
  purrr[transpose]
)

# Build only for orgs that are `curr_client` and have `polaris_is_active`.
orgs <- orgIndex()[curr_client == TRUE]
orgs <- orgs[short_name %in% plAppDataIndex()[polaris_is_active == TRUE, short_name]]

# Read from integrated, write to appdata.
pg_in <- dbc(Sys.getenv("HCA_ENV", "prod2"), "integrated")
pg_out <- dbc(Sys.getenv("HCA_ENV", "prod2"), "appdata")

# Get org-wide variables.
blocked_carriers <- tbl(pg_in, "sms_blocked_carriers") |>
  select(carrier) |>
  collect()
phones_carriers <- tbl(pg_in, "phones_carriers") |>
  left_join(tbl(pg_in, "normalized_phone_carriers"), by = "carrier") |>
  collect() |>
  arrange(desc(updated_at_utc)) |>
  distinct(phone, .keep_all = TRUE) |>
  select(last_customer_phone = phone, carrier, normalized_carrier)

# Build and write org-by-org.
lapply(transpose(orgs), function(org) {
  message("Building ", org$short_name)
  customer_opt_status <- tbl(pg_in, "optin_status") |>
    filter(orguuid == !!org$org_uuid) |>
    transmute(phone, opt_in = if_else(status == "opt_in", "Y", "N"))
  polaris_phones_info <- tbl(pg_in, "customer_sms") |>
    filter(org == !!org$short_name) |>
    full_join(customer_opt_status, by = "phone") |>
    transmute(
      last_customer_phone = phone, last_sms = last_sms_engagement_utc, tot_sms = tot_sms_received,
      latest_campaign_id = last_campaign_id, latest_tot_sms = tot_sms_received, opt_in
    ) |>
    collect() |>
    left_join(phones_carriers, by = "last_customer_phone") |>
    mutate(blocked_by_carrier = carrier %in% !!blocked_carriers$carrier, org = !!org$short_name) |>
    distinct(last_customer_phone, .keep_all = TRUE)
  dbBegin(pg_out)
  dbExecute(pg_out, paste0("DELETE FROM polaris_phones_info WHERE org = '", org$short_name, "'"))
  dbAppendTable(pg_out, "polaris_phones_info", polaris_phones_info)
  dbCommit(pg_out)
})

dbd(pg_in)
dbd(pg_out)
