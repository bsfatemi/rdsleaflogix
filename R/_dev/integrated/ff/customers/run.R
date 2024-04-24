# Integrated Customers
# run.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

box::use(
  dplyr[filter],
  hcaconfig[dbc, dbd, orgIndex],
  hcapipelines[plIndex],
  DBI[dbBegin, dbCommit, dbExecute, dbExistsTable, dbGetQuery, dbListFields, dbWriteTable],
  parallel[detectCores, mclapply],
  pipelinetools[db_read_table_unique, check_cols],
  jsonlite[fromJSON, toJSON],
  stringr[str_glue]
)

source("inst/pipeline/integrated/customers/src/build_customers.R")

# Known for lawsuits against dispensaries, banned for every dispensary
global_optout <- c("+18182036544")

(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))

# Grab current clients' information
current_clients <- orgIndex()[curr_client == TRUE, org_uuid]
filtered_orgs <- unique(filter(plIndex(), org_uuid %in% current_clients, pos_data == TRUE))
orgs <- fromJSON(toJSON(filtered_orgs), simplifyDataFrame = FALSE)

## Run integrated customers org by org
cust_result <- mclapply(orgs, function(org) {
  try({
    pg <- dbc(db_cfg, "integrated")
    opt_outs <- dbGetQuery(pg, str_glue("
      SELECT
        org, phone, is_subscribed, tot_sms_received, last_sms_engagement_utc, resubscribed_at_utc,
        unsubscribed_at_utc, last_customer_resp, last_msg_received, last_campaign_id
      FROM customer_sms
      WHERE org = '{org$short_name}'
    "))
    dbd(pg)

    # Check that the org's consolidated customers table exists
    conn <- dbc(db_cfg, "consolidated")
    if (dbExistsTable(conn, org$customers)) {
      customers <- db_read_table_unique(conn, org$customers, "customer_id", "source_run_date_utc")
      dbd(conn)

      # Build org customer table
      customers <- build_customers(customers, opt_outs) |>
        filter(!phone %in% global_optout)

      # SQL safety in case the connection is interrupted
      integrated <- dbc(db_cfg, "integrated")
      customers <- check_cols(customers, dbListFields(integrated, "customers"))
      dbBegin(integrated)
      dbExecute(integrated, str_glue("DELETE FROM customers where org = '{org$short_name}'"))
      dbWriteTable(integrated, "customers", customers, append = TRUE)
      dbCommit(integrated)
      dbd(integrated)
    } else {
      dbd(conn)
      stop(paste("TABLE DOES NOT EXIST:", org$customers))
    }
  })
}, mc.cores = min(3L, detectCores()))

# Maintenance done after all orgs are added to the table
pg <- dbc(db_cfg, "integrated")
dbExecute(pg, "VACUUM ANALYZE customers")
dbd(pg)

errored_orgs <- sapply(orgs, function(x) x$short_name)[
  sapply(cust_result, function(x) inherits(x, "try-error"))
]
# Notify fail in case any of the orgs fail with customers tables present
if (length(errored_orgs) > 0) {
  stop("Integrated customers build failed for: ", paste(errored_orgs, collapse = ", "))
}
