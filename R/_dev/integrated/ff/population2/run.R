# Integrated Population2
# run.R
#
# (C) 2021 Happy Cabbage Analytics, Inc.

# Source ----------------------------------------------------------------------

box::use(
  data.table[as.data.table],
  DBI[dbGetQuery, dbBegin, dbCommit, dbExecute, dbWriteTable],
  hcaconfig[dbc, dbd, orgIndex, orgShortName],
  hcapipelines[plIndex, get_pipeline_child_process_count],
  logger[log_info],
  parallel[mclapply]
)

log_info("Begin population2 build script")
source("inst/pipeline/integrated/population2/src/build_population2.R")

# Read conf -------------------------------------------------------------------
orgs_uuids <- orgIndex()[in_pop == TRUE]
# Calculate population just for orgs `in_pop` & `curr_client`.
filtered_orgs <- plIndex()[org_uuid %in% orgs_uuids[curr_client == TRUE, unique(org_uuid)], .SD]
# Build and write data --------------------------------------------------------
log_info("Build population2 for current orgs")

population_res <- mclapply(filtered_orgs$org_uuid, function(org_uuid) {
  # Don't fail if couldn't fully execute.
  try({
    # Build population2 for one org.
    org_short_name <- orgShortName(org_uuid)
    log_info(paste("Reading in pop1 table for org:", org_short_name, "org_uuid", org_uuid))
    cn <- dbc(Sys.getenv("HCA_ENV", "prod2"), "integrated")
    DT <- dbGetQuery(cn, paste0("SELECT
        org_uuid,
        order_time_utc,
        order_line_total,
        order_line_subtotal,
        order_line_discount,
        order_line_tax,
        order_line_list_price,
        order_total,
        order_subtotal,
        order_discount,
        order_id,
        order_line_id,
        product_id,
        raw_category_name,
        category3,
        product_name,
        brand_name,
        product_category_name,
        product_class,
        product_sku,
        product_qty,
        order_facility,
        facility,
        store_id,
        age,
        birthday,
        order_type,
        phone,
        gender,
        email,
        first_name,
        last_name,
        customer_id
        FROM population WHERE org_uuid = '", org_uuid, "'"))
    log_info(paste("Building pop2 for org:", org_short_name, "org_uuid", org_uuid))
    POP2 <- build_population2(as.data.table(DT))
    log_info(paste("Built pop2 for org:", org_short_name, "org_uuid", org_uuid))
    dbBegin(cn)
    log_info("Deleting old rows from population2")
    dbExecute(cn, paste0("DELETE FROM population2 WHERE org_uuid = '", org_uuid, "'"))
    log_info("Writing the org's population2 to database.")
    dbWriteTable(cn, "population2", POP2, append = TRUE)
    dbCommit(cn)
    dbd(cn)
  })
}, mc.cores = get_pipeline_child_process_count())
log_info("Finished building population2")
cn <- dbc(Sys.getenv("HCA_ENV", "prod2"), "integrated")
log_info("Delete rows for inactive orgs.")
dbExecute(cn, paste0(
  "DELETE FROM population2 WHERE org_uuid NOT IN ('",
  paste(filtered_orgs$org_uuid, collapse = "','"),
  "')"
))
# Database maintenance for population2 ----------------------------------------
dbd(cn)

# Notify errors ---------------------------------------------------------------
errored_orgs <- filtered_orgs$short_name[
  sapply(population_res, function(x) inherits(x, "try-error"))
]
if (length(errored_orgs) > 0) {
  stop("Population2 build failed for: ", paste(errored_orgs, collapse = ", "))
}
log_info("Finish population2 build script.")
