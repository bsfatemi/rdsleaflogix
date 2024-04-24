# Integrated Attribution
# run.R
#
# (C) Happy Cabbage Analytics, Inc. 2021
box::use(
  hcaconfig[dbc, dbd, orgShortName, orgIndex],
  DBI[dbGetQuery, dbWriteTable, dbExecute, dbBegin, dbCommit],
  stringr[str_glue],
  lubridate[ymd_hms],
  dplyr[left_join, bind_rows, pull],
  parallel[mclapply, detectCores],
  hcapipelines[plAppDataIndex],
  purrr[transpose]
)

# Source ------------------------------------------------------------------
source(
  "inst/pipeline/integrated/attribution/attributed_sms_order_lines/src/build_sms_roi_attribution.R",
  local = TRUE
)

## Import ------------------------------------------------------------------
(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))
out_table <- "sms_attributed_order_lines"

## All unique orgs that have sent campaigns
campaign_orgs <- plAppDataIndex()[
  current_client == TRUE & polaris_is_active == TRUE, .(org_uuid, short_name)
]

# Start building campaign attribution data org by org
attr_res <- mclapply(transpose(campaign_orgs), function(org_row) {
  ## Don't fail if it couldn't fully execute
  try({
    short_name <- org_row$short_name
    org_uuid <- org_row$org_uuid

    # More precise logging since print statements don't appear in the raw cron logs
    message(paste("Starting sms attribution for:", short_name, org_uuid))

    ad <- dbc(db_cfg, "appdata")
    campaigns <- dbGetQuery(ad, str_glue(
      "SELECT campaign_id, campaign_body, org
  FROM mstudio_user_campaigns
  WHERE org = '{short_name}'"
    ))
    dbd(ad)

    integrated <- dbc(db_cfg, "integrated")
    check <- dbGetQuery(integrated, str_glue(
      "SELECT count(*) from population where org = '{short_name}'"
    )) |> pull(count)
    dbd(integrated)


    # Skip over orgs with no SMS data or sales data
    if (nrow(campaigns) == 0 || check == 0) {
      return(invisible(NULL))
    }

    cp <- dbc(db_cfg, "cabbage_patch")
    # Pull in historical twilio data
    tw_sms <- dbGetQuery(
      cp,
      str_glue(
        "SELECT
      tw.to,
      tw.body,
      tw.date_sent,
      tw.sid AS msg_id,
      tw.org_uuid,
      tw.org,
      logs.campaign_id
    FROM tw_sms AS tw
    LEFT JOIN (
      SELECT DISTINCT
        campaign_id,
        msg_id
      FROM mstudio_sms_logs
      WHERE sms_service = 'twilio'
      AND org = '{short_name}'
    ) AS logs
    ON tw.sid = logs.msg_id
    WHERE
          tw.status != 'failed'
      AND tw.sent_from_messaging_service = TRUE
      AND tw.org = '{short_name}'"
      )
    )

    # Pull in telnyx data
    tel_sms <- dbGetQuery(
      cp,
      str_glue(
        "SELECT
      tel.to_1_phone_number      AS to,
      tel.text                   AS body,
      tel.sent_at                AS date_sent,
      tel.id       AS msg_id,
      tel.orguuid  AS org_uuid,
      logs.campaign_id
    FROM tel_sms AS tel
    LEFT JOIN (
      SELECT DISTINCT
        campaign_id,
        msg_id
      FROM mstudio_sms_logs
      WHERE sms_service = 'telnyx'
      AND org = '{short_name}'
    ) AS logs
    ON tel.id = logs.msg_id
    WHERE
          tel.errors_1_code IS NULL
      AND tel.direction = 'outbound'
      AND tel.orguuid = '{org_uuid}'"
      )
    )
    dbd(cp)

    # Mimicking earlier logic
    guidmap <- orgIndex()[, .(org = short_name, org_uuid)]
    tel_sms$date_sent <- ymd_hms(tel_sms$date_sent)
    tel_sms <- left_join(tel_sms, guidmap)
    sms <- bind_rows(tw_sms, tel_sms)

    # Just in case these are massive tables, NULL them out to save memory
    tw_sms <- NULL
    tel_sms <- NULL

    # Pull in order data
    integrated <- dbc(db_cfg, "integrated")
    # Using 'org' or short_name to take advantage of population index
    population <- dbGetQuery(integrated, str_glue("
      SELECT
        customer_id, org, source_system, phone, order_id,
        order_type, order_facility, order_time_utc, order_time_local, order_total,
        order_line_id, product_name, product_id, brand_name, product_uom,
        product_unit_count, product_qty, product_category_name, product_class,
        order_line_total, category3
      FROM population
      WHERE org = '{short_name}'
    "))

    sms_lines <- build_sms_roi_attribution(sms, campaigns, population)
    # SQL safety in case this action fails
    dbBegin(integrated)
    # Using short_name because org_uuid is not a column of sms_attributed_order_lines
    dbExecute(integrated, str_glue("DELETE FROM {out_table} WHERE org = '{short_name}'"))
    dbWriteTable(integrated, out_table, sms_lines, append = TRUE)
    dbCommit(integrated)
    dbd(integrated)
    message(paste("Finished sms attribution for:", short_name, org_uuid))
  })
}, mc.cores = min(4L, detectCores()))

integrated <- dbc(db_cfg, "integrated")
dbExecute(integrated, paste("VACUUM ANALYZE", out_table))
dbd(integrated)

# Write -------------------------------------------------------------------
errored_orgs <- campaign_orgs[sapply(attr_res, function(x) inherits(x, "try-error"))]$short_name
if (length(errored_orgs) > 0) {
  stop("Attribution build failed for: ", paste(errored_orgs, collapse = ", "))
}
