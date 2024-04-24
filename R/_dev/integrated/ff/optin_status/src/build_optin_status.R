box::use(
  dplyr[
    anti_join, arrange, bind_rows, case_when, coalesce, desc, distinct, filter, group_by, if_else,
    left_join, mutate, rename, row_number, select, transmute, ungroup
  ],
  hcaconfig[orgIndex],
  hcapipelines[plAppDataIndex],
  jsonlite[fromJSON],
  lubridate[as_datetime, ymd_hms],
  pipelinetools[process_phone],
  purrr[map],
  tidyr[unnest_wider]
)

#' Per org / phone, creates the SMS opt-status.
#'
#' Prioritizes opt-status by channel sms_response > custom_code > hca_webhook > file_import > pos .
#'
#' @param sms_response Opt-ins data coming from SMSs responses.
#' @param custom_code Opt-ins data coming from polaris appdata custom code.
#' @param webhook Opt-ins data coming from filled forms.
#' @param file_import Opt-ins data coming from an org's submitted flat file.
#' @param pos Opt-ins data coming from the POS system.
#'
build_optin_status <- function(sms_response, custom_code, webhook, file_import, pos) {
  ## Opt-In Status from SMS
  sms_response <- filter(
    sms_response, !is.na(orguuid), !is.na(unsubscribed_at_utc) | !is.na(resubscribed_at_utc)
  ) |>
    transmute(
      orguuid,
      phone   = process_phone(phone),
      ts      = as_datetime(if_else(is_subscribed, resubscribed_at_utc, unsubscribed_at_utc)),
      channel = "sms_response",
      status  = if_else(is_subscribed, "opt_in", "opt_out")
    ) |>
    filter(!is.na(phone))
  optins_table <- sms_response
  rm(sms_response)

  ## Opt-In Status from Custom Code
  custom_code <- transmute(custom_code, orguuid, phone, ts, channel = "custom_code", status) |>
    filter(!is.na(phone)) |>
    anti_join(optins_table, by = c("orguuid", "phone"))
  optins_table <- bind_rows(optins_table, custom_code)
  rm(custom_code)

  ## Opt-In Status from Webhook
  webhook <- transmute(
    webhook, orguuid, phone,
    ts = ts_gmt, channel = "hca_webhook", status = "opt_in"
  ) |>
    anti_join(optins_table, by = c("orguuid", "phone"))
  optins_table <- bind_rows(optins_table, webhook)
  rm(webhook)

  ## Opt-In Status from Flat File
  file_import <- transmute(
    file_import, orguuid, phone,
    ts = time, channel = "file_import",
    status = if_else(opted_in, "opt_in", "opt_out")
  ) |>
    anti_join(optins_table, by = c("orguuid", "phone"))
  optins_table <- bind_rows(optins_table, file_import)
  rm(file_import)

  ## Opt-In Status from POS System
  pos <- left_join(pos, orgIndex()[, c("short_name", "org_uuid")], by = c(org = "short_name")) |>
    filter(!is.na(phone)) |>
    anti_join(optins_table, by = c(org_uuid = "orguuid", "phone")) |>
    transmute(
      orguuid = org_uuid,
      phone,
      ts = coalesce(ymd_hms(last_updated_utc), as_datetime(0)), # Assume oldest status if no info
      channel = "pos",
      status = case_when(
        pos_is_subscribed == FALSE ~ "opt_out",
        pos_is_subscribed == TRUE ~ "opt_in",
        TRUE ~ "opt_out"
      )
    ) |>
    group_by(phone, orguuid) |>
    filter(row_number(ts) == max(row_number(ts))) |>
    ungroup()
  optins_table <- bind_rows(optins_table, pos)
  rm(pos)

  # Keep the most recent time by the highest ranked channel.
  filter(optins_table, orguuid %in% filter(plAppDataIndex(), polaris_is_active)$org_uuid) |>
    arrange(channel, desc(ts)) |>
    distinct(.keep_all = TRUE)
}
