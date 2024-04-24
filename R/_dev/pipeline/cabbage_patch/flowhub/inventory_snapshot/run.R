# source ------------------------------------------------------------------
source("inst/pipeline/cabbage_patch/flowhub/inventory_snapshot/init.R", local = TRUE)

box::use(
  DBI[dbExecute, dbWriteTable],
  dplyr[distinct, filter],
  hcaconfig[dbc, dbd, orgIndex],
  lubridate[floor_date, now],
  purrr[map_dfr, transpose]
)

# run ---------------------------------------------------------------------
snapshot_utc <- floor_date(now("UTC"), "30 minutes")
start <- Sys.time()
org_index <- orgIndex()
curr_clients <- org_index[curr_client == TRUE, org_uuid]
kx <- filter(keyIndex(), org_uuid %in% curr_clients)

# start querying.
rooms_stock <- map_dfr(transpose(kx), inventory_snapshot) |>
  distinct() # In case an org had multiple facilities, but it had to query the general endpoint.
rooms_stock$snapshot_utc <- snapshot_utc

end <- Sys.time()
print(end - start)

# write/append ------------------------------------------------------------
cn <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
dbWriteTable(cn, "flowhub_rooms_stock_snapshot", rooms_stock, append = TRUE)
dbExecute(cn, "ANALYZE flowhub_rooms_stock_snapshot")
dbd(cn)
