# Collect Telnyx Messages
# run.R
#
# (C) Happy Cabbage Analytics 2021

# Log IDs
strt <- lubridate::now("UTC") - lubridate::hours(2) # perform sync in lagging 1 hour windows
end <- lubridate::now("UTC") - lubridate::hours(1)

pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
log_ids <- DBI::dbGetQuery(pg, stringr::str_glue(
  "SELECT DISTINCT
    msg_id AS id, org
  FROM mstudio_sms_logs
  WHERE
    sms_service = 'telnyx'
    AND msg_id IS NOT NULL
    AND created_at_utc BETWEEN '{strt}' AND '{end}'
  "
))
hcaconfig::dbd(pg)

log_ids <- hcaconfig::orgIndex() |>
  dplyr::inner_join(log_ids, by = c("short_name" = "org")) |>
  dplyr::select(id, org_uuid)

# LOOP THROUGH AND QUERY
orgs <- hcapipelines::plAppDataIndex()[polaris_is_active == TRUE]$org_uuid
auths <- purrr::map(orgs, hcaconfig::orgAuth, .api = "telnyx")
names(auths) <- orgs
auths <- purrr::compact(auths)


res <- purrr::map(log_ids$id, function(i) {
  if (which(log_ids$id %in% i) %% 100 == 0) {
    print(stringr::str_glue("Syncing {which(log_ids$id %in% i)} out of {nrow(log_ids)} ..."))
  }
  smstools::..tl_getv2(
    ep = stringr::str_glue("messages/{i}"), auths[[as.character(log_ids[log_ids$id == i, 2])]]$key
  )
})
ll <- purrr::map_chr(res, httr::content, as = "text", encoding = "UTF-8")
pipelinetools::storeExternalRaw(
  ll, name = "tel_sms", clientName = "happycabbage", dSource = "telnyx"
)
