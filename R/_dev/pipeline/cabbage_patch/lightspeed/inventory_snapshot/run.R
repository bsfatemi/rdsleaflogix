# source ------------------------------------------------------------------
source("inst/pipeline/cabbage_patch/lightspeed/inventory_snapshot/init.R", local = TRUE)

box::use(
  DBI[dbAppendTable, dbExecute],
  dplyr[filter],
  hcaconfig[dbc, dbd, orgIndex],
  hcalightspeed[get_api_keys],
  lubridate[floor_date, now],
  purrr[map_dfr, transpose]
)

# run ---------------------------------------------------------------------
snapshot_utc <- floor_date(now("UTC"), "30 minutes")
start <- Sys.time()
org_index <- orgIndex()
curr_clients <- org_index[curr_client == TRUE, org_uuid]
kx <- filter(get_api_keys(), org_uuid %in% curr_clients)

result <- map_dfr(transpose(kx), inventory_snapshot)
result$snapshot_utc <- snapshot_utc

end <- Sys.time()
print(end - start)

# write/append ------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
dbAppendTable(pg, "lightspeed_rooms_stock_snapshot", result)
dbExecute(pg, "ANALYZE lightspeed_rooms_stock_snapshot")
dbd(pg)
