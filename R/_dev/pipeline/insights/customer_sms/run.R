# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbAppendTable, dbBegin, dbCommit, dbExecute, dbGetQuery],
  dplyr[bind_rows, coalesce, mutate],
  glue[glue],
  hcaconfig[dbc, dbd, orgIndex],
  lubridate[now, ymd_hms],
  parallel[detectCores, mclapply],
  purrr[transpose]
)

# Source ------------------------------------------------------------------
source("inst/pipeline/insights/customer_sms/src/build_customer_sms.R", local = TRUE)

# Get the orgs to build customers data.
out_table <- "customer_sms"
curr_clients <- orgIndex()[curr_client == TRUE, ]
(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))
run_date_utc <- now("UTC")

# Build -------------------------------------------------------------------
mclapply(transpose(curr_clients), function(org) {
  cp <- dbc(db_cfg, "cabbage_patch")
  tw_sms <- dbGetQuery(cp, glue("
    SELECT
      tw.from, tw.to, tw.sent_from_messaging_service, tw.body, tw.date_sent, tw.sid AS msg_id,
      tw.org_uuid, tw.org, logs.campaign_id
    FROM (
      (SELECT * FROM tw_sms WHERE org = '{org$short_name}') AS tw
      LEFT JOIN (
        SELECT DISTINCT campaign_id, msg_id
        FROM mstudio_sms_logs
        WHERE sms_service = 'twilio' AND org = '{org$short_name}'
      ) AS logs
      ON tw.sid = logs.msg_id
    )
  "))
  tel_sms <- dbGetQuery(cp, glue("
    SELECT
      tel.from_phone_number AS from, tel.to_1_phone_number AS to,
      tel.direction = 'outbound' AS sent_from_messaging_service, tel.text AS body,
      COALESCE(tel.sent_at, tel.received_at) AS date_sent, tel.id AS msg_id,
      tel.orguuid AS org_uuid, logs.campaign_id
    FROM (
      (SELECT * FROM tel_sms WHERE orguuid = '{org$org_uuid}') AS tel
      LEFT JOIN (
        SELECT DISTINCT campaign_id, msg_id
        FROM mstudio_sms_logs
        WHERE sms_service = 'telnyx' AND org = '{org$short_name}'
      ) AS logs
      ON tel.id = logs.msg_id
    )
    WHERE tel.errors_1_code IS NULL
  "))
  dbd(cp)
  # Some cleaning.
  tel_sms$date_sent <- ymd_hms(tel_sms$date_sent)
  tw_sms <- mutate(tw_sms, guid = !!org$org_uuid, org_uuid = coalesce(org_uuid, guid))
  # Build.
  customer_sms <- build_customer_sms(bind_rows(tel_sms, tw_sms))
  # Clean up.
  customer_sms$run_date_utc <- run_date_utc
  customer_sms$org <- org$short_name
  # Write to db.
  pg <- dbc(db_cfg, "integrated")
  dbBegin(pg)
  dbExecute(pg, glue("DELETE FROM {out_table} WHERE org = '{org$short_name}'"))
  dbAppendTable(pg, out_table, customer_sms)
  dbCommit(pg)
  dbd(pg)
}, mc.cores = min(4, detectCores()))

# DB maintenance ----------------------------------------------------------
pg <- dbc(db_cfg, "integrated")
dbExecute(pg, glue("VACUUM ANALYZE {out_table}"))
dbd(pg)
