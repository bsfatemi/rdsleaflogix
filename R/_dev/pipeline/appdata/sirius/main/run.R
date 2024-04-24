# Sirius App Data
# run.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

# Source ------------------------------------------------------------------
source("inst/pipeline/appdata/sirius/main/src/build_sirius_appdata.R", local = TRUE)
source("inst/pipeline/appdata/sirius/main/src/build_demo_sirius_appdata.R", local = TRUE)

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbWriteTable, SQL],
  hcaconfig[dbc, dbd, lookupOrgGuid],
  hcapipelines[plAppDataIndex, get_pipeline_child_process_count],
  parallel[mclapply],
  logger[log_info, log_success],
  stringr[str_glue]
)

# cfg to connect in `hcaconfig::dbc`, "prod2" by default if "HCA_ENV" not set.
(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))
orgs <- plAppDataIndex()[sirius_is_active == TRUE, unique(short_name)]

log_info("Sirius build for all orgs starting.")
## RUN
##
## run the pipeline process
##
sirius_res <- mclapply(orgs, function(org_name) {
  # Don't fail if couldn't fully execute.
  try({
    log_info("Building sirius for:", org_name)
    conn_in <- dbc(db_cfg, "integrated")
    population <- dbGetQuery(conn_in, str_glue("SELECT order_time_local,
    delivery_order_received_local, delivery_order_started_local, delivery_order_complete_local,
    first_name, last_name, gender, customer_city, order_total, order_subtotal,
    delivery_order_address, delivery_order_city, delivery_order_zipcode, customer_zipcode,
    customer_id, phone, email, age, birthday, order_id, sold_by_id, sold_by, order_facility,
    order_type, org, order_line_id, product_id, brand_id, category3, product_name,
    product_class, product_qty, product_unit_count, brand_name, order_line_total,
    order_line_subtotal, order_line_list_price, order_line_tax, order_source, order_line_discount
                                               FROM population WHERE org = '{org_name}' AND
                                               order_facility IS NOT NULL"))
    dbd(conn_in)
    srs_appdata <- build_sirius_appdata(population)
    srs_appdata$org_uuid <- lookupOrgGuid(org_name)
    conn_out <- dbc(db_cfg, "appdata")
    dbBegin(conn_out)
    dbExecute(conn_out, str_glue('DELETE FROM  "sirius"."main" WHERE org = \'{org_name}\''))
    dbWriteTable(conn_out, SQL("sirius.main"), srs_appdata, append = TRUE)
    dbCommit(conn_out)
    dbd(conn_out)
    log_info("finished sirius for:", org_name)
  })
}, mc.cores = get_pipeline_child_process_count())


# DEMO DATASET
conn_in <- dbc(db_cfg, "appdata")
demo_polaris <- dbGetQuery(conn_in, "SELECT * FROM polaris_appdata WHERE org = 'demo'")
medithrive_appdata <- dbGetQuery(conn_in, '
  SELECT * FROM "sirius"."main" WHERE org_uuid = \'0ec7399e-395c-4b17-b287-5abd20e957ee\' and
  order_time >= \'2020-01-01 00:00:00\'
')
dbd(conn_in)
demo_srs_appdata <- build_demo_sirius_appdata(medithrive_appdata, demo_polaris)
demo_srs_appdata$org_uuid <- lookupOrgGuid("demo")

conn_out <- dbc(db_cfg, "appdata")
dbBegin(conn_out)
dbExecute(conn_out, str_glue('DELETE FROM  "sirius"."main" WHERE org = \'demo\''))
dbWriteTable(conn_out, SQL("sirius.main"), demo_srs_appdata, append = TRUE)
dbCommit(conn_out)

log_info("Deleting sirius data for inactive orgs")
# Delete rows for inactive orgs.
dbExecute(conn_out, paste0(
  'DELETE FROM "sirius"."main" WHERE org NOT IN (\'',
  paste(c(orgs, "demo"), collapse = "', '"),
  "')"
))
log_info("finished deleting sirius data for inactive orgs")
dbd(conn_out)

errored_orgs <- orgs[sapply(sirius_res, function(x) inherits(x, "try-error"))]
if (length(errored_orgs) > 0) {
  stop("Sirius build failed for: ", paste(errored_orgs, collapse = ", "))
}
