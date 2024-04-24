# Polaris App Data
# run.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

# Source ------------------------------------------------------------------
source("inst/pipeline/appdata/polaris/mstudio_sms_logs/init.R", local = TRUE)
source("inst/pipeline/appdata/polaris/mstudio_sms_logs/src/build_mstudio_sms_logs.R", local = TRUE)

pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "appdata")
df <- DBI::dbReadTable(pg, "mstudio_sent_sms")
hcaconfig::dbd(pg)

if (nrow(df) == 0) {
  warning(paste(Sys.time(), "No New Sent SMS to Log"))
} else {
  logs <- build_mstudio_sms_logs(df)
  logs$msg_id <- logs$data_id

  pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
  cols <- DBI::dbListFields(pg, "mstudio_sms_logs")
  DBI::dbAppendTable(pg, "mstudio_sms_logs", dplyr::select(logs, dplyr::one_of(cols)))
  hcaconfig::dbd(pg)

  pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "appdata")
  DBI::dbExecute(pg, stringr::str_glue(
    "DELETE FROM mstudio_sent_sms
     WHERE id <= {max(logs$id)}
     AND   id >= {min(logs$id)}
    "
  ))
  hcaconfig::dbd(pg)
}
