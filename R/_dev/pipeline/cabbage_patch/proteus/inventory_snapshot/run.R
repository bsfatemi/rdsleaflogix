# source ------------------------------------------------------------------
# dir necessary due to the current way we run pipelines as non-installed files
source("inst/pipeline/cabbage_patch/proteus/inventory_snapshot/init.R", local = TRUE)

box::use(
  DBI[dbExecute, dbWriteTable],
  dplyr[bind_rows, filter, mutate_at],
  hcaconfig[dbc, dbd, orgIndex],
  lubridate[floor_date, now],
  purrr[map, map_lgl, transpose]
)

# run ---------------------------------------------------------------------
snapshot_utc <- floor_date(now("UTC"), "30 minutes")
start <- Sys.time()
org_index <- orgIndex()
curr_clients <- org_index[curr_client == TRUE, org_uuid]
kx <- filter(keyIndex(), org_uuid %in% curr_clients)

result <- map(transpose(kx), try(inventory_snapshot))
errored <- kx$org_uuid[map_lgl(result, ~ inherits(.x, "try-error"))]
result <- result[!map_lgl(result, ~ inherits(.x, "try-error"))] |>
  bind_rows() |>
  mutate_at(c("product_id", "brand_id", "room_id"), as.character)
result$snapshot_utc <- snapshot_utc

end <- Sys.time()
print(end - start)

# write/append ------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
dbWriteTable(pg, "proteus_rooms_stock_snapshot", result, append = TRUE)
dbExecute(pg, "ANALYZE proteus_rooms_stock_snapshot")
dbd(pg)

# error messaging ---------------------------------------------------------
if (length(errored) > 0) {
  stop("Proteus Stock Snapshot failed for ", paste0(errored, collapse = ", "))
}
