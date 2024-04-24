library(DBI)
library(dplyr)

pg <- do.call(DBI::dbConnect, hcaconfig::ld_odbc("prod2", "appdata"))
usercamps <- DBI::dbGetQuery(
  pg,
  "SELECT
    created_at_utc,
    org,
    campaign_id,
    campaign_body,
    campaign_source,
    media_url,
    scheduled_time_utc,
    approx_targets_n,
    useruuid
  FROM
    mstudio_user_campaigns
  WHERE date(created_at_utc) >= '2021-01-01'"
)
DBI::dbDisconnect(pg)


pg <- do.call(DBI::dbConnect, hcaconfig::ld_odbc("prod2", "cabbage_patch"))
sendLogs <- DBI::dbGetQuery(
  pg,
  "SELECT
    count(*) send_logs_n,
    campaign_id,
    org,
    min(created_at_utc) start_log,
    max(created_at_utc) end_log,
    http_code,
    http_status_category,
    http_status_message
  FROM
    mstudio_sms_logs
  WHERE date(created_at_utc) >= '2021-01-01'
  GROUP BY
    campaign_id,
    org,
    http_code,
    http_status_category,
    http_status_message"
)
tw_sub_qry <-
  "SELECT
    tw.from,
    tw.to,
    tw.sent_from_messaging_service,
    tw.body,
    tw.date_sent,
    tw.sid AS msg_id,
    tw.org_uuid,
    tw.org,
    cast(tw.num_media as integer) > 0 AS is_mms,
    tw.price,
    cast(tw.num_segments as integer) AS num_segments,
    tw.error_code,
    logs.campaign_id
  FROM (
    SELECT
      \"from\", \"to\", sent_from_messaging_service, body, sid, date_sent,
      org_uuid, org, num_media, price, num_segments, error_code
    FROM tw_sms
    UNION ALL
    SELECT
      \"from\", \"to\", sent_from_messaging_service, body, sid, date_sent,
      org_uuid, org, num_media, price, num_segments, error_code
    FROM tw_errors
  ) AS tw
  LEFT JOIN (
    SELECT DISTINCT
      campaign_id,
      msg_id
    FROM mstudio_sms_logs
    WHERE sms_service = 'twilio'
  ) AS logs
  ON tw.sid = logs.msg_id
  WHERE tw.date_sent > '2021-01-01'"

tw <- DBI::dbGetQuery(pg, stringr::str_glue(
  "SELECT
      count(*) sms_service_n,
      campaign_id,
      min(date_sent) start_sent,
      max(date_sent) end_sent,
      error_code
    FROM ({tw_sub_qry}) tw
    WHERE campaign_id IS NOT NULL
    GROUP BY
      campaign_id,
      error_code"
))
tw$sms_service <- "twilio"

tl_sub_qry <-
  "SELECT
    tel.from_phone_number      AS from,
    tel.to_1_phone_number      AS to,
    tel.direction = 'outbound' AS sent_from_messaging_service,
    tel.text                   AS body,
    tel.sent_at                AS date_sent,
    tel.id            AS msg_id,
    tel.orguuid       AS org_uuid,
    tel.type = 'MMS'  AS is_mms,
    cast(tel.cost_cost as double precision) AS price,
    tel.parts         AS num_segments,
    tel.errors_1_code AS error_code,
    CASE WHEN tel.direction = 'outbound' THEN to_1_carrier ELSE from_carrier END AS carrier,
    logs.campaign_id
  FROM tel_sms AS tel
  LEFT JOIN (
    SELECT DISTINCT
      campaign_id,
      msg_id
    FROM mstudio_sms_logs
    WHERE sms_service = 'telnyx'
  ) AS logs
  ON tel.id = logs.msg_id"

tl <- DBI::dbGetQuery(pg, stringr::str_glue(
  "SELECT
      count(*) sms_service_n,
      campaign_id,
      min(date_sent) start_sent,
      max(date_sent) end_sent,
      error_code,
      carrier
    FROM ({tl_sub_qry}) tl
    WHERE campaign_id IS NOT NULL
    GROUP BY
      campaign_id,
      error_code,
      carrier"
))
tl$sms_service <- "telnyx"
DBI::dbDisconnect(pg)

tl$start_sent <- lubridate::ymd_hms(tl$start_sent)
tl$end_sent <- lubridate::ymd_hms(tl$end_sent)

# JOIN
joined <- usercamps %>%
  left_join(sendLogs) %>%
  left_join(bind_rows(tl, tw))

prd <- do.call(DBI::dbConnect, hcaconfig::ld_odbc("prod2", "appdata"))
DBI::dbBegin(prd)
DBI::dbExecute(prd, paste("TRUNCATE TABLE", DBI::SQL("sol.delivery_rates")))
DBI::dbWriteTable(prd, DBI::SQL("sol.delivery_rates"), joined, append = TRUE)
DBI::dbCommit(prd)
DBI::dbDisconnect(prd)
