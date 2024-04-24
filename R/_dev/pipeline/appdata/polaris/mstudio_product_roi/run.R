# Polaris - Agg. Product ROI
# run.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

box::use(
  DBI[dbGetQuery, dbWriteTable, dbExecute, dbBegin, dbCommit],
  hcaconfig[dbc, dbd, orgIndex],
  hcapipelines[plAppDataIndex],
  parallel[mclapply, detectCores],
  stringr[str_glue]
)

# Source ------------------------------------------------------------------
source(
  "inst/pipeline/appdata/polaris/mstudio_product_roi/src/build_mstudio_product_roi.R", local = TRUE
)
source("inst/pipeline/appdata/polaris/mstudio_product_roi/src/anonymize_demoorg.R")

(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))
out_table <- "mstudio_product_roi"
current_clients <- orgIndex()[curr_client == TRUE, short_name]
orgs <- plAppDataIndex()[polaris_is_active == TRUE & short_name %in% current_clients, short_name]
# Demoorg is going to be handled in a different manner.
orgs <- orgs[orgs != "demoorg"]

# Make --------------------------------------------------------------------
#
result <- mclapply(orgs, function(org) {
  pg <- dbc(db_cfg, "integrated")
  attr_lines <- dbGetQuery(pg, str_glue("
    SELECT
      last_sms_campaign_id, last_sms_body, product_name, product_category_name, brand_name, org,
      order_line_total, is_24hr, is_72hr, is_7day, is_2wk, is_30day, category3
    FROM sms_attributed_order_lines
    WHERE org = '{org}'
  "))
  dbd(pg)

  roi <- build_mstudio_product_roi(attr_lines)

  ad <- dbc(db_cfg, "appdata")
  dbBegin(ad)
  dbExecute(ad, str_glue("DELETE FROM {out_table} where org = '{org}'"))
  dbWriteTable(ad, out_table, roi, append = TRUE)
  dbCommit(ad)
  dbd(ad)
}, mc.cores = min(4L, detectCores()))


# Demoorg's fake data (it gets data from MMD's Hollywood, Long Beach, Marina Del Rey, and NoHo).
ad <- dbc(db_cfg, "appdata")
# Obtain all possible campaigns from MMD.
demoorg_campaigns <- dbGetQuery(
  ad, "SELECT campaign_id FROM mstudio_user_campaigns WHERE org = 'mmd'"
)
demoorg_data <- dbGetQuery(ad, str_glue("SELECT * FROM {out_table} WHERE org = 'mmd'")) |>
  anonymize_demoorg("demoorg", demoorg_campaigns)
dbBegin(ad)
dbExecute(ad, str_glue("DELETE FROM {out_table} WHERE org = 'demoorg'"))
dbWriteTable(ad, out_table, demoorg_data, append = TRUE)
dbCommit(ad)
dbd(ad)

# DB Maintenance ----------------------------------------------------------
ad <- dbc(db_cfg, "appdata")
dbExecute(ad, str_glue("VACUUM ANALYZE {out_table}"))
dbd(ad)
