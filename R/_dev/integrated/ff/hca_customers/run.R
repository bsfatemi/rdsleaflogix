box::use(
  data.table[as.data.table, `:=`],
  DBI[dbGetQuery, dbWriteTable],
  hcaconfig[dbc, dbd, orgIndex],
  hcapipelines[plIndex],
  lubridate[now],
  purrr[transpose],
  stringr[str_glue],
  uuid[UUIDgenerate]
)

# Variables ---------------------------------------------------------------

(db_env <- Sys.getenv("HCA_ENV", "prod2"))

current_clients <- unique(orgIndex()[curr_client == TRUE, org_uuid])
client_tables <- plIndex()[org_uuid %in% current_clients, .(org_uuid, short_name, customers)]

loop_through <- transpose(client_tables)
# Run ---------------------------------------------------------------------

hca_customer_res <- lapply(loop_through, function(org) {
  try({
    org_uuid <- org$org_uuid
    customers <- org$customers
    cn <- dbc(db_env, "consolidated")

    qry <- str_glue('SELECT customer_id, full_name, phone, email FROM "{customers}"')
    DT <- as.data.table(dbGetQuery(cn, qry))
    dbd(cn)

    pg <- dbc(db_env, "integrated")
    process_qry <- str_glue("SELECT phone
                          FROM hca_customers_by_org
                          WHERE org_uuid = '{org_uuid}'")
    processed_customers <- dbGetQuery(pg, process_qry)
    dbd(pg)

    ## Filter consolidated customers by non-NA phone
    to_process <- DT[!is.na(phone), .(customer_id, full_name, phone, email)]

    ## Filter out customers with already processed phone numbers
    to_process <- to_process[!processed_customers, on = "phone"]

    ## Collapse different pos_customer_ids into one regex for easy querying
    result <- to_process[, pos_customer_id := paste0(customer_id, collapse = "|"), by = .(phone)]
    result[, customer_id := NULL]

    ## Fill in missing information
    result[, hca_customer_id := UUIDgenerate(), phone]
    result[, org_uuid := org_uuid]
    result[, run_date_utc := now()]

    ## Explicit columns to return
    ## Note: Should return (nrow(customers) / {number_of_stores}) roughly for multi-store Treez
    ## clients
    unique_cols <- c("org_uuid", "hca_customer_id", "pos_customer_id", "phone")
    result <- unique(result, by = unique_cols)

    ## Write
    cn <- dbc(db_env, "integrated")
    dbWriteTable(cn, "hca_customers_by_org", result, append = TRUE)
    dbd(cn)
  })
})

# Error Reporting ---------------------------------------------------------

errored_orgs <- sapply(loop_through, function(x) x$short_name)[
  sapply(hca_customer_res, function(x) inherits(x, "try-error"))
]
if (length(errored_orgs) > 0) {
  stop("HCA Customer Ids build failed for: ", paste(errored_orgs, collapse = ", "))
}
