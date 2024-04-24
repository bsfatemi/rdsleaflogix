# Sirius Campaign Data
# run.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

# Source ------------------------------------------------------------------
source("inst/pipeline/appdata/sirius/campaign/init.R", local = TRUE)
source("inst/pipeline/appdata/sirius/campaign/src/build_srs_campaign.R", local = TRUE)
source("inst/pipeline/appdata/sirius/campaign/src/anonymize_demoorg.R")

# cfg to connect in `hcaconfig::dbc`, "prod2" by default if "HCA_ENV" not set.
(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))

# READ --------------------------------------------------------------------
pg <- hcaconfig::dbc(db_cfg, "appdata")
raw <- DBI::dbGetQuery(pg, stringr::str_glue(
  "SELECT
    c.*,
    u.campaign_source,
    u.filters_applied
   FROM mstudio_sms_campaigns AS c
   LEFT JOIN mstudio_user_campaigns AS u
   ON c.campaign_id = u.campaign_id"
))
# Obtain all possible campaigns from MMD, to fake for demoorg.
demoorg_campaigns <- DBI::dbGetQuery(
  pg, "SELECT campaign_id FROM mstudio_user_campaigns WHERE org = 'mmd'"
)
hcaconfig::dbd(pg)

# RUN --------------------------------------------------------------------
data <- build_srs_campaign(raw)

# Demoorg's fake data (it gets data from MMD's Hollywood, Long Beach, Marina Del Rey, and NoHo).
demoorg_sad <- dplyr::filter(data, org_uuid == hcaconfig::lookupOrgGuid("mmd")) |>
  anonymize_demoorg("demoorg", demoorg_campaigns)
data <- dplyr::bind_rows(data, demoorg_sad)

# WRITE --------------------------------------------------------------------
pg <- hcaconfig::dbc(db_cfg, "appdata")
DBI::dbExecute(pg, paste("TRUNCATE TABLE", SQL("sirius.campaign")))
DBI::dbWriteTable(pg, SQL("sirius.campaign"), data, append = TRUE)
hcaconfig::dbd(pg)
