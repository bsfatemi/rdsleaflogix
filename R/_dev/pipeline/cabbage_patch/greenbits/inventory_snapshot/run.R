# source ------------------------------------------------------------------
source("inst/pipeline/cabbage_patch/greenbits/inventory_snapshot/init.R", local = TRUE)

box::use(
  DBI[dbAppendTable, dbExecute],
  dplyr[bind_rows, filter],
  hcaconfig[dbc, dbd, orgIndex],
  hcagreenbits[get_api_keys],
  lubridate[floor_date, now],
  purrr[map, map_lgl, transpose]
)

# run ---------------------------------------------------------------------
snapshot_utc <- floor_date(now("UTC"), "30 minutes")
start <- Sys.time()
org_index <- orgIndex()
curr_clients <- org_index[curr_client == TRUE, org_uuid]
kx <- filter(get_api_keys(), org_uuid %in% curr_clients)

result <- map(transpose(kx), ~ try(inventory_snapshot(.x), silent = TRUE))
errored <- kx[map_lgl(result, ~ inherits(.x, "try-error")), ]
result <- bind_rows(result[!map_lgl(result, ~ inherits(.x, "try-error"))])
result$snapshot_utc <- snapshot_utc

end <- Sys.time()
print(end - start)

# write/append ------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
dbAppendTable(pg, "greenbits_rooms_stock_snapshot", result)
dbExecute(pg, "ANALYZE greenbits_rooms_stock_snapshot")
dbd(pg)

# errors notification -----------------------------------------------------
if (nrow(errored) > 0) {
  message(
    "Failed to obtain snapshots for ",
    paste(errored$org_short_name, errored$store_short_name, collapse = ", "), "; succeeded for ",
    nrow(kx) - nrow(errored)
  )
}

if (nrow(errored) == nrow(kx) && nrow(kx) > 0) {
  stop("Failed to obtain snapshots for every customer.")
}
