# Integrated Attribution
# run.R
#
# (C) Happy Cabbage Analytics, Inc. 2021
box::use(
  DBI[dbAppendTable, dbBegin, dbCommit, dbExecute, dbGetQuery],
  dplyr[filter, mutate, transmute],
  glue[glue],
  hcaconfig[dbc, dbd],
  hcapipelines[plAppDataIndex],
  lubridate[as_datetime],
  parallel[detectCores, mclapply],
  polarispub[get_email_campaign],
  purrr[map_dfr, transpose]
)

source(
  paste0(
    "inst/pipeline/integrated/attribution/attributed_email_order_lines/src/",
    "build_email_roi_attribution.R"
  )
)

## Import ------------------------------------------------------------------
(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))
out_table <- "email_attributed_order_lines"

# All unique orgs that could have sent campaigns.
campaign_orgs <- plAppDataIndex()[
  current_client == TRUE & polaris_is_active == TRUE, .(org_uuid, short_name)
]

# Start building campaign attribution data org by org.
attr_res <- mclapply(transpose(campaign_orgs), function(org_row) {
  ## Don't fail if it couldn't fully execute.
  try({
    short_name <- org_row$short_name
    org_uuid <- org_row$org_uuid

    message(paste("Starting email attribution for:", short_name, org_uuid))

    pg <- dbc(db_cfg, "appdata")
    campaigns <- dbGetQuery(pg, glue("
      SELECT
        campaign_uuid, archx_campaign_id, email_subject, email_img_path, email_body,
        email_cta_btn_text, email_cta_url, email_template_id
      FROM mstudio_scheduled_email_campaigns
      WHERE org_uuid = '{org_uuid}' AND status = 'sent'
    "))
    dbd(pg)

    # Skip over orgs with no email campaigns sent.
    if (nrow(campaigns) == 0) {
      return(invisible(NULL))
    }

    sent_emails <- map_dfr(campaigns$archx_campaign_id, function(archx_campaign_id) {
      email_campaign <- get_email_campaign(archx_campaign_id)
      if ("error" %in% names(email_campaign)) {
        warning("Couldn't obtain Archx campaign ", archx_campaign_id, ": ", email_campaign$error)
        email_campaign <- data.frame()
      }
      email_campaign
    }) |>
      filter(emailSent) |>
      transmute(
        campaign_id = campaignId, sent_at_utc = as_datetime(updatedAt), email = toupper(email)
      )

    # Pull in order data.
    pg <- dbc(db_cfg, "integrated")
    population <- dbGetQuery(pg, glue("
      SELECT
        customer_id, org, source_system, email, order_id, order_type, order_facility,
        order_time_utc, order_time_local, order_total, order_line_id, product_name, product_id,
        brand_name, product_uom, product_unit_count, product_qty, product_category_name,
        product_class, order_line_total
      FROM population
      WHERE org = '{short_name}'
    ")) |>
      mutate(email = toupper(email))

    email_lines <- build_email_roi_attribution(sent_emails, campaigns, population)
    # SQL safety in case this action fails.
    dbBegin(pg)
    dbExecute(pg, glue("DELETE FROM {out_table} WHERE org = '{short_name}'"))
    dbAppendTable(pg, out_table, email_lines)
    dbCommit(pg)
    dbd(pg)
    message(paste("Finished sms attribution for:", short_name, org_uuid))
  })
}, mc.cores = min(4L, detectCores()))

pg <- dbc(db_cfg, "integrated")
dbExecute(pg, paste("VACUUM ANALYZE", out_table))
dbd(pg)

# Write -------------------------------------------------------------------
errored_orgs <- campaign_orgs[sapply(attr_res, function(x) inherits(x, "try-error"))]$short_name
if (length(errored_orgs) > 0) {
  stop("Attribution build failed for: ", paste(errored_orgs, collapse = ", "))
}
