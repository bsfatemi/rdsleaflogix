# Polaris - Agg. Product ROI
# run.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

box::use(
  DBI[dbAppendTable, dbBegin, dbCommit, dbExecute, dbGetQuery],
  glue[glue],
  hcaconfig[dbc, dbd, orgIndex],
  hcapipelines[plAppDataIndex],
  parallel[mclapply, detectCores]
)

source(
  "inst/pipeline/appdata/polaris/mstudio_email_product_roi/src/build_mstudio_email_product_roi.R"
)

(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))
out_table <- "mstudio_email_product_roi"
current_clients <- orgIndex()[curr_client == TRUE, short_name]
orgs <- plAppDataIndex()[polaris_is_active == TRUE & short_name %in% current_clients, short_name]

# Make --------------------------------------------------------------------
result <- mclapply(orgs, function(org) {
  pg <- dbc(db_cfg, "integrated")
  attr_lines <- dbGetQuery(pg, glue("
    SELECT
      last_email_campaign_uuid, last_email_subject, last_email_body, last_email_img_path,
      last_email_template_id, product_name, product_category_name, brand_name,
      order_line_total, is_24hr, is_72hr, is_7day, is_2wk, is_30day
    FROM email_attributed_order_lines
    WHERE org = '{org}'
  "))
  dbd(pg)

  roi <- build_mstudio_email_product_roi(attr_lines)
  roi$org <- org

  pg <- dbc(db_cfg, "appdata")
  dbBegin(pg)
  dbExecute(pg, glue("DELETE FROM {out_table} where org = '{org}'"))
  dbAppendTable(pg, out_table, roi)
  dbCommit(pg)
  dbd(pg)
}, mc.cores = min(4L, detectCores()))

# DB Maintenance ----------------------------------------------------------
pg <- dbc(db_cfg, "appdata")
dbExecute(pg, glue("VACUUM ANALYZE {out_table}"))
dbd(pg)
