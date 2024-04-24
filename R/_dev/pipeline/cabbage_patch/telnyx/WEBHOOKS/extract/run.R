library("magrittr")

# VARS
out_table <- "tel_sms"

# RUN
json <- pipelinetools::rd_raw_archive("webhook", "happycabbage", "telnyx_webhooks")
msgs <- smstools::tl_extract_raw_messages(json)

# Process only if there are messages.
if (nrow(msgs) == 0) {
  warning("No Telnyx Webhooks obtained. Execution time: ", Sys.time())
} else if (nrow(msgs) > 0) {
  # Webhooks shoot multiple events per message, so we need to de-dupe
  msgs <- msgs %>%
    dplyr::group_by(id) %>%
    dplyr::filter(
      as.integer(webhook_event_type) == min(as.integer(webhook_event_type))
    ) %>%
    dplyr::filter(
      lubridate::as_datetime(webhook_occurred_at) ==
        max(lubridate::as_datetime(webhook_occurred_at))
    ) %>%
    dplyr::distinct() %>%
    dplyr::ungroup()

  # ORGS
  orgs <- unique(hcaconfig::orgIndex()$org_uuid)
  auths <- purrr::map(orgs, hcaconfig::orgAuth, .api = "telnyx")
  names(auths) <- orgs
  authdt <- data.table::rbindlist(auths, idcol = "orguuid")
  msgs <- dplyr::left_join(
    msgs,
    dplyr::select(authdt, messaging_profile_id = mp, orguuid),
    by = "messaging_profile_id"
  ) %>%
    # Remove any dupes by `id`, keeps most recent `webhook_occurred_at`.
    dplyr::arrange(desc(lubridate::as_datetime(webhook_occurred_at))) %>%
    dplyr::distinct(id, .keep_all = TRUE)

  # WRITE
  pg <- hcaconfig::dbc("prod2", "cabbage_patch")
  msgs <- pipelinetools::check_cols(msgs, DBI::dbListFields(pg, out_table))
  # using current hour and minute to temp table to keep unique name
  temp_table_name <- paste0(
    "telnyx_webhook", lubridate::hour(lubridate::now()),
    lubridate::minute(lubridate::now())
  )
  out_table <- "tel_sms"
  DBI::dbWriteTable(pg, temp_table_name, dplyr::distinct(msgs, id), temporary = TRUE)
  DBI::dbBegin(pg)
  DBI::dbExecute(pg, paste0(
    'DELETE FROM "', out_table, '" WHERE id IN (SELECT id FROM "',
    temp_table_name, '")'
  ))
  DBI::dbWriteTable(pg, out_table, msgs, append = TRUE)
  DBI::dbCommit(pg)
  hcaconfig::dbd(pg)
}
