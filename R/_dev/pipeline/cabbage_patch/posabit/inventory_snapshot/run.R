# source ----------------------------------------------------------------------
source("inst/pipeline/cabbage_patch/posabit/inventory_snapshot/init.R", local = TRUE)

# libraries -------------------------------------------------------------------
box::use(
  DBI[dbAppendTable, dbExecute],
  dplyr[bind_rows, filter],
  hcaconfig[dbc, dbd, orgIndex],
  hcaposabit[get_api_keys],
  lubridate[floor_date, now],
  purrr[map]
)

# variables -------------------------------------------------------------------
snapshot_utc <- floor_date(now("UTC"), "30 minutes")
org_index <- orgIndex()
curr_clients <- org_index[curr_client == TRUE, org_uuid]
curr_clients <- filter(get_api_keys(), org_uuid %in% curr_clients)

# get -------------------------------------------------------------------------
stock_snapshot <- map(seq_len(nrow(curr_clients)), function(i) {
  message(
    "Processing... ", curr_clients$org_short_name[[i]], " - ", curr_clients$store_short_name[[i]]
  )
  try(get_stock_snapshot(curr_clients[i, ]), silent = TRUE)
})

# check errors ----------------------------------------------------------------
errored <- curr_clients$org_short_name[sapply(stock_snapshot, function(x) inherits(x, "try-error"))]
if (length(errored) > 0 && length(errored) == nrow(curr_clients)) {
  stop("POSaBIT stock snapshot failed for all orgs (", length(errored), ").")
}

# save ------------------------------------------------------------------------
stock_snapshot <- bind_rows(stock_snapshot)
stock_snapshot$snapshot_utc <- snapshot_utc
cn <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
dbAppendTable(cn, "posabit_rooms_stock_snapshot", stock_snapshot)
dbExecute(cn, "ANALYZE posabit_rooms_stock_snapshot")
dbd(cn)
