box::use(
  DBI[dbExecute],
  lubridate[now, floor_date, hours],
  hcaconfig[dbc, dbd],
  stringr[str_glue]
)

(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))
"start_utc" <- floor_date(now(tzone = "UTC") - hours(13))

pg <- dbc(db_cfg, "cabbage_patch")
dbExecute(pg, str_glue(
  "DELETE FROM mstudio_sms_exclusions
  WHERE (org, campaign_id, phone) IN (SELECT org, campaign_id, \"to\" as phone
FROM (SELECT msg_id AS id, to_1_status, created_at_utc, org, campaign_id, http_code, \"to\"
FROM (SELECT id, to_1_status
FROM tel_sms
WHERE (to_1_status in ('delivery_failed', 'sending_failed') AND direction = 'outbound')) LHS
RIGHT JOIN (SELECT *
FROM (SELECT created_at_utc, org, campaign_id, http_code, \"to\", msg_id
FROM mstudio_sms_logs) q01
WHERE (created_at_utc >= '{start_utc}')) RHS
ON (LHS.id = RHS.msg_id)
) q02
WHERE (http_code != '200' OR to_1_status in ('delivery_failed', 'sending_failed')))"
))

"old_time_utc" <- floor_date(now(tzone = "UTC") - hours(24))
dbExecute(pg, str_glue("DELETE FROM mstudio_sms_exclusions WHERE timestamp < '{old_time_utc}'"))
dbExecute(pg, "VACUUM mstudio_sms_exclusions")
dbd(pg)
