# RUN
json <- pipelinetools::rd_raw_archive("tel_sms", "happycabbage", "telnyx")
js <- purrr::keep(json, ~ !stringr::str_detect(.x, "Message not found"))

if (length(js) == 0) {
  warning("NO MESSAGES SYNC'D")
} else {
  msgs <- smstools::tl_extract_raw_messages(js)
  msgs <- dplyr::filter(msgs, !is.na(id))

  # ORGS
  orgs <- unique(hcaconfig::orgIndex()$org_uuid)
  auths <- purrr::map(orgs, hcaconfig::orgAuth, .api = "telnyx")
  names(auths) <- orgs
  authdt <- data.table::rbindlist(auths, idcol = "orguuid")
  msgs <- dplyr::left_join(msgs, dplyr::select(authdt, messaging_profile_id = mp, orguuid))

  # WRITE ------------------------------------------------------------
  if (nrow(msgs) > 0) {
    pg <- hcaconfig::dbc("prod2", "cabbage_patch")
    temp_table_name <- "telnyx_asynch"
    out_table <- "tel_sms"
    flds <- DBI::dbListFields(pg, "tel_sms")
    msgs <- dplyr::select(msgs, one_of(flds))
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
}
