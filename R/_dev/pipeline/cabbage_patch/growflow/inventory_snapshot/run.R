# source ------------------------------------------------------------------
source("inst/pipeline/cabbage_patch/growflow/inventory_snapshot/init.R", local = TRUE)

box::use(
  dplyr[bind_rows, filter, mutate_at],
  DBI[dbExecute, dbWriteTable],
  hcagrowflow[get_api_keys],
  hcaconfig[dbc, dbd, orgIndex],
  lubridate[floor_date, now],
  purrr[map, map_lgl, transpose],
)

# run bind and clean ------------------------------------------------------
snapshot_utc <- floor_date(now("UTC"), "30 minutes")
start <- Sys.time()
org_index <- orgIndex()
curr_clients <- org_index[curr_client == TRUE, org_uuid]

gf_apikeys <- get_api_keys() |>
  filter(org_uuid %in% curr_clients)

result <- map(transpose(gf_apikeys), try(inventory_snapshot))
errored <- gf_apikeys$org_uuid[map_lgl(result, ~ inherits(.x, "try-error"))]
result <- result[!map_lgl(result, ~ inherits(.x, "try-error"))] |>
  bind_rows() |>
  mutate_at(c("product_id", "brand_id", "room_id"), as.character)
result$snapshot_utc <- snapshot_utc

end <- Sys.time()
print(end - start)

# write out ---------------------------------------------------------------
cn <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
dbWriteTable(cn, "growflow_rooms_stock_snapshot", result, append = TRUE)
dbExecute(cn, "ANALYZE growflow_rooms_stock_snapshot")
dbd(cn)

# error messaging ---------------------------------------------------------
if (length(errored) > 0) {
  stop("Growflow Stock Snapshot failed for ", paste0(errored, collapse = ", "))
}
