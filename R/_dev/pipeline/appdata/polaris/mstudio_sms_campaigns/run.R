# Polaris - Agg. Campaigns
# run.R
#
# (C) Happy Cabbage Analytics, Inc. 2021
box::use(
  hcaconfig[dbc, dbd, orgIndex],
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbWriteTable],
  stringr[str_glue],
  dplyr[bind_rows, distinct, left_join, rename, `%>%`],
  lubridate[days, today, ymd_hms],
  parallel[detectCores, mclapply],
  purrr[transpose]
)

# Source ------------------------------------------------------------------

source(
  "inst/pipeline/appdata/polaris/mstudio_sms_campaigns/src/build_mstudio_sms_campaigns.R",
  local = TRUE
)
source("inst/pipeline/appdata/polaris/mstudio_sms_campaigns/src/anonymize_demoorg.R")

(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))
out_table <- "mstudio_sms_campaigns"
curr_clients <- orgIndex()[curr_client == TRUE, .(org = short_name, org_uuid)]
# Demoorg is going to be handled in a different manner.
curr_clients <- curr_clients[org != "demoorg"]

result <- mclapply(transpose(curr_clients), function(org) {
  try({
    ad <- dbc(db_cfg, "appdata")
    campaign_info <- dbGetQuery(ad, str_glue("
      SELECT campaign_id, campaign_body, org, media_url
      FROM mstudio_user_campaigns WHERE org IN ('{org$org}')
    "))
    dbd(ad)

    # SMS info from Twilio including campaign_id from mstudio_sms_logs
    # And info on errors from tw_errors and tw_sms
    # To process most of the data merging on the SQL side we use dbplyr
    cp <- dbc(db_cfg, "cabbage_patch")
    tw_sms <- dbGetQuery(cp, str_glue("
      SELECT
        tw.from, tw.to, tw.sent_from_messaging_service, tw.body, tw.date_sent, tw.sid AS msg_id,
        tw.org_uuid, tw.org, CAST(tw.num_media as integer) > 0 AS is_mms, tw.price,
        CAST(tw.num_segments as integer) AS num_segments, tw.error_code, logs.campaign_id
      FROM (
        SELECT
          \"from\", \"to\", sent_from_messaging_service, body, sid, date_sent, org_uuid, org,
          num_media, price, num_segments, error_code
        FROM tw_sms
        WHERE org_uuid IN ('{org$org_uuid}')
        UNION ALL
        SELECT
          \"from\", \"to\", sent_from_messaging_service, body, sid, date_sent, org_uuid, org,
          num_media, price, num_segments, error_code
        FROM tw_errors
        WHERE org_uuid = '{org$org_uuid}'
      ) AS tw
      INNER JOIN (
        SELECT DISTINCT campaign_id, msg_id
        FROM mstudio_sms_logs
        WHERE sms_service = 'twilio' AND org = '{org$org}'
      ) AS logs
      ON tw.sid = logs.msg_id
      WHERE tw.date_sent > '2021-01-01'
    "))

    tel_sms <- dbGetQuery(cp, str_glue("
      SELECT
        tel.from_phone_number AS from, tel.to_1_phone_number AS to,
        tel.direction = 'outbound' AS sent_from_messaging_service, tel.text AS body,
        tel.sent_at AS date_sent, tel.id AS msg_id, tel.orguuid AS org_uuid,
        tel.type = 'MMS' AS is_mms, CAST(tel.cost_cost as double precision) AS price,
        tel.parts AS num_segments, tel.errors_1_code AS error_code, logs.campaign_id
      FROM (
        SELECT from_phone_number, to_1_phone_number, direction,
          text, sent_at, id, orguuid, type, cost_cost, parts,
          errors_1_code
        FROM tel_sms
        WHERE orguuid = '{org$org_uuid}'
      ) AS tel
      INNER JOIN (
        SELECT DISTINCT campaign_id, msg_id
        FROM mstudio_sms_logs
        WHERE sms_service = 'telnyx' AND org = '{org$org}'
      ) AS logs
      ON tel.id = logs.msg_id
    "))
    dbd(cp)

    # ROI information
    pg <- dbc(db_cfg, "integrated")
    sms_attributed_order_lines <- dbGetQuery(pg, str_glue("
      SELECT DISTINCT
        last_sms_campaign_id, org, customer_id, order_total, is_24hr, is_72hr, is_7day, is_2wk,
        is_30day, was_lost
      FROM sms_attributed_order_lines WHERE org = '{org$org}'
    "))
    # Latest data on opt-outs by campaign_id.
    # Based on data in customer_sms.
    opt_out_info <- dbGetQuery(pg, str_glue("
      SELECT
        org, last_campaign_id AS campaign_id,
        SUM(CAST(is_subscribed = 'FALSE' AS INTEGER)) AS unsubscribes
      FROM customer_sms WHERE org = '{org$org}'
      GROUP BY campaign_id, org
    "))
    dbd(pg)

    # CLEAN COLS
    tel_sms$date_sent <- ymd_hms(tel_sms$date_sent)
    tel_sms <- left_join(tel_sms, curr_clients, by = "org_uuid")

    conn <- dbc(db_cfg, "hcaconfig")
    polaris_config <- dbGetQuery(conn, str_glue("
      SELECT org_uuid, mms_ratio
      FROM org_polaris_config
      WHERE org_uuid = '{org$org_uuid}'
    "))
    dbd(conn)
    polaris_config <- polaris_config %>%
      left_join(orgIndex(), by = "org_uuid") %>%
      distinct(short_name, mms_ratio) %>%
      rename("org" = "short_name")


    # Make --------------------------------------------------------------------
    campaigns <- build_mstudio_sms_campaigns(
      bind_rows(tel_sms, tw_sms),
      campaign_info,
      opt_out_info,
      sms_attributed_order_lines,
      polaris_config
    )

    ad <- dbc(db_cfg, "appdata")
    dbBegin(ad)
    dbExecute(ad, str_glue("DELETE FROM {out_table} WHERE org = '{org$org}'"))
    if (org$org == "medithrive") {
      dbExecute(ad, str_glue("DELETE FROM {out_table} WHERE org = 'demo'"))
    }
    dbWriteTable(ad, name = out_table, value = campaigns, append = TRUE)
    dbCommit(ad)
    dbd(ad)
  })
}, mc.cores = min(4, detectCores()))


# Demoorg's fake data (it gets data from MMD's Hollywood, Long Beach, Marina Del Rey, and NoHo).
ad <- dbc(db_cfg, "appdata")
# Obtain all possible campaigns from MMD.
demoorg_campaigns <- dbGetQuery(
  ad,
  # Do not pull future campaigns to avoid campaign_id collision
  str_glue(
    "SELECT * FROM mstudio_user_campaigns
     WHERE org = 'mmd' AND scheduled_time_utc < '{today() - days(1)}'"
  )
)
demoorg_pad <- dbGetQuery(ad, str_glue(
  "SELECT * FROM {out_table} WHERE org = 'mmd' AND last_text < '{today() - days(1)}'"
))
anon_data <- anonymize_demoorg(demoorg_pad, "demoorg", demoorg_campaigns)
dbBegin(ad)
dbExecute(ad, "DELETE FROM mstudio_user_campaigns WHERE org = 'demoorg'")
dbWriteTable(ad, "mstudio_user_campaigns", anon_data$demoorg_campaigns, append = TRUE)
dbExecute(ad, str_glue("DELETE FROM {out_table} WHERE org = 'demoorg'"))
dbWriteTable(ad, out_table, anon_data$demoorg_pad, append = TRUE)
dbCommit(ad)
dbd(ad)

# DB Maintenance ----------------------------------------------------------
ad <- dbc(db_cfg, "appdata")
dbExecute(ad, str_glue("VACUUM ANALYZE {out_table}"))
dbd(ad)

# Notify errors -------------------------------------------------------------
errored_orgs <- curr_clients$org[sapply(result, function(x) inherits(x, "try-error"))]
if (length(errored_orgs) > 0) {
  stop("mstudio_sms_campaigns build failed for: ", paste(errored_orgs, collapse = ", "))
}
