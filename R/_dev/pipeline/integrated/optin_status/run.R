## optin_status
## run.R
##
## (C) Happy Cabbage Analytics 2021

## SOURCE
source("inst/pipeline/integrated/optin_status/src/build_optin_status.R")

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  dplyr[rename],
  hcaconfig[dbc, dbd],
  pipelinetools[db_read_table_unique]
)


active_orgs <- plAppDataIndex() |> filter(current_client == TRUE & polaris_is_active == TRUE)
(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))

## READ
pg <- dbc(db_cfg, "cabbage_patch")
dbWriteTable(pg, "cp_orgs_temp",
  active_orgs |> distinct(org_uuid),
  temporary = TRUE
)
webhook <- db_read_table_unique(
  pg, "form_optins2", c("phone", "orguuid"), "ts_gmt",
  columns = c("phone", "orguuid", "ts_gmt", "custom_tags"),
  extra_filters = paste0("orguuid IN (SELECT org_uuid FROM cp_orgs_temp)")
)
# Our one api  https://cabbage.pub/v1/optin/R/hca/json only sends us opt-ins as a phone and org_uuid
# that api will not send us any info if they are not opting in so if custom_tags is empty they are
# opt-ins since they could only be from that API
webhook$custom_tags <- replace(
  webhook$custom_tags, is.na(webhook$custom_tags),
  '{"name":[""],"email":{},"attendant":{},"rating":[],"comment":{},"status":["opt_in"]}'
)

# Only grabs opt-ins as per discussion with Danny and Bobby not opting in does not mean opt-out
webhook <- webhook |>
  mutate(
    json_parsed = map(custom_tags, ~ fromJSON(., flatten = TRUE))
  ) |>
  unnest_wider(json_parsed) |>
  filter(status == "opt_in") |>
  select("phone", "orguuid", "ts_gmt")

file_import <- db_read_table_unique(
  pg, "flatfile_optins", c("phone", "orguuid"), "time",
  columns = c("phone", "orguuid", "opted_in", "time"),
  extra_filters = paste0("orguuid IN (SELECT org_uuid FROM cp_orgs_temp)")
)
dbd(pg)

pg <- dbc(db_cfg, "integrated")
dbWriteTable(pg, "int_orgs_temp",
  active_orgs |> distinct(short_name, org_uuid),
  temporary = TRUE
)
pos <- db_read_table_unique(
  pg, "customers", c("phone", "org"), "last_updated_utc",
  columns = c("phone", "org", "pos_is_subscribed", "last_updated_utc"),
  extra_filters = paste0("org IN (SELECT short_name FROM int_orgs_temp)")
)
sms_response <- db_read_table_unique(
  pg, "customer_sms", c("phone", "org_uuid"), "run_date_utc",
  columns = c("phone", "org_uuid", "is_subscribed", "resubscribed_at_utc", "unsubscribed_at_utc"),
  extra_filters = paste0("org_uuid IN (SELECT org_uuid FROM int_orgs_temp)")
) |>
  rename("orguuid" = "org_uuid")
custom_code <- db_read_table_unique(
  pg, "custom_code_optin_status", c("orguuid", "phone"), "ts",
  extra_filters = paste0("orguuid IN (SELECT org_uuid FROM int_orgs_temp)")
)
dbd(pg)

## RUN
optin_status <- build_optin_status(sms_response, custom_code, webhook, file_import, pos)

## WRITE
pg <- dbc(db_cfg, "integrated")
# Atomic db overwrite.
dbBegin(pg)
dbExecute(pg, "TRUNCATE optin_status")
dbWriteTable(pg, "optin_status", optin_status, append = TRUE)
dbCommit(pg)
dbd(pg)
