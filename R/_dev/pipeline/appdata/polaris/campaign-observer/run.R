# Polaris API
# run.R
#
# (C) Happy Cabbage Analytics, Inc. 2021
source("inst/pipeline/appdata/polaris/campaign-observer/init.R", local = TRUE)
# httr::set_config(httr::config(ssl_verifypeer = 0L))

# RUN OBSERVER
orgs <- get_orgs_in_queue()
print(stringr::str_glue(
  "Running Campaign Observer at {Sys.time()} on PID {Sys.getpid()} for orgs ",
  "{paste(orgs, collapse = ', ')}"
))
failed_runs <- lapply(orgs, function(org) {
  ll <- runPeriodCampaigns(period_mins = 30, org)
  cn <- do.call(DBI::dbConnect, hcaconfig::ld_odbc("prod2", "appdata"))

  # Log Campaigns
  if (length(ll$smsTargets) > 0) {
    res <- DBI::dbWriteTable(cn, name = "mstudio_sent_sms", value = ll$smsTargets, append = TRUE)
  } else {
    res <- 0
  }

  # Log Staff
  if (length(ll$smsStaff) > 0) {
    if (nrow(ll$smsStaff)) {
      res2 <- DBI::dbWriteTable(cn, name = "mstudio_sent_sms", value = ll$smsStaff, append = TRUE)
    } else {
      res2 <- 0
    }
  } else {
    res2 <- 0
  }

  # Clean up and log process
  DBI::dbDisconnect(cn)
  print(paste(
    Sys.time(),
    "run_campaign_observer sms logs for cids:",
    paste(unique(ll$smsTargets$campaign_id), collapse = ", ")
  ))

  return(ll$errors)
})


if (length(purrr::compact(failed_runs)) > 0) {
  stop(purrr::compact(failed_runs))
}
