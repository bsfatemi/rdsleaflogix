# Polaris - Agg. Campaigns
# run.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

box::use(
  hcaconfig[dbc, dbd, orgIndex],
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbAppendTable],
  dplyr[full_join, group_by, n, summarise, transmute],
  glue[glue],
  lubridate[as_datetime],
  parallel[detectCores, mclapply],
  polarispub[get_email_campaign, get_email_campaign_stats],
  purrr[map_dfr, transpose]
)

source(
  "inst/pipeline/appdata/polaris/mstudio_email_campaigns/src/build_mstudio_email_campaigns.R"
)

(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))
out_table <- "mstudio_email_campaigns"
curr_clients <- orgIndex()[curr_client == TRUE, .(org = short_name, org_uuid)]

result <- mclapply(transpose(curr_clients), function(org) {
  try({
    pg <- dbc(db_cfg, "appdata")
    campaign_info <- dbGetQuery(pg, glue("
      SELECT
        campaign_uuid, archx_campaign_id, email_subject, email_img_path, email_body,
        email_cta_btn_text, email_cta_url, email_template_id
      FROM mstudio_scheduled_email_campaigns
      WHERE org_uuid = '{org$org_uuid}' AND status = 'sent'
    "))
    dbd(pg)

    # Skip over orgs with no email campaigns sent.
    if (nrow(campaign_info) == 0) {
      return(invisible(NULL))
    }

    # Get campaign targets info.
    sent_emails_info <- map_dfr(campaign_info$archx_campaign_id, function(archx_campaign_id) {
      email_campaign <- get_email_campaign(archx_campaign_id)
      if ("error" %in% names(email_campaign)) {
        warning("Couldn't obtain Archx campaign ", archx_campaign_id, ": ", email_campaign$error)
        email_campaign <- data.frame()
      }
      email_campaign
    }) |>
      group_by(campaign_id = campaignId) |>
      summarise(
        sent_at_utc = min(as_datetime(updatedAt), na.rm = TRUE),
        end_at_utc = max(as_datetime(updatedAt), na.rm = TRUE),
        .groups = "drop"
      )
    # Get campaign stats.
    sent_emails_info_stats <- map_dfr(campaign_info$archx_campaign_id, function(archx_campaign_id) {
      email_campaign <- get_email_campaign_stats(archx_campaign_id)
      if ("error" %in% names(email_campaign)) {
        warning("Couldn't obtain Archx campaign ", archx_campaign_id, ": ", email_campaign$error)
        email_campaign <- data.frame()
      } else {
        email_campaign <- data.frame(email_campaign)
        email_campaign$campaign_id <- archx_campaign_id
      }
      email_campaign
    })
    # Merge campaign stats.
    sent_emails_info <- full_join(sent_emails_info, sent_emails_info_stats, by = "campaign_id") |>
      transmute(
        campaign_id, sent_at_utc, end_at_utc,
        customers_reached = total,
        delivery_rate = delivered / total, open_rate = opened / total, click_rate = clicked / total
      )

    # ROI information.
    pg <- dbc(db_cfg, "integrated")
    email_attributed_order_lines <- dbGetQuery(pg, glue("
      SELECT DISTINCT
        last_email_campaign_uuid, customer_id, order_total, is_24hr, is_72hr, is_7day, is_2wk,
        is_30day, was_lost
      FROM email_attributed_order_lines WHERE org = '{org$org}'
    "))
    dbd(pg)

    # Make --------------------------------------------------------------------
    campaigns <- build_mstudio_email_campaigns(
      campaign_info, sent_emails_info, email_attributed_order_lines
    )
    campaigns$org <- org$org

    pg <- dbc(db_cfg, "appdata")
    dbBegin(pg)
    dbExecute(pg, glue("DELETE FROM {out_table} WHERE org = '{org$org}'"))
    dbAppendTable(pg, out_table, campaigns)
    dbCommit(pg)
    dbd(pg)
  })
}, mc.cores = min(4, detectCores()))

# DB Maintenance ----------------------------------------------------------
pg <- dbc(db_cfg, "appdata")
dbExecute(pg, glue("VACUUM ANALYZE {out_table}"))
dbd(pg)

# Notify errors -------------------------------------------------------------
errored_orgs <- curr_clients$org[sapply(result, function(x) inherits(x, "try-error"))]
if (length(errored_orgs) > 0) {
  stop("mstudio_sms_campaigns build failed for: ", paste(errored_orgs, collapse = ", "))
}
