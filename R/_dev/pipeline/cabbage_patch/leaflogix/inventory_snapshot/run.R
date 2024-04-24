# source ------------------------------------------------------------------
# dir necessary due to the current way we run pipelines as non-installed files
source("inst/pipeline/cabbage_patch/leaflogix/inventory_snapshot/init.R", local = TRUE)

box::use(
  DBI[dbWriteTable],
  dplyr[filter, mutate_at, select],
  hcaconfig[dbc, dbd, orgIndex],
  purrr[map_dfr, transpose]
)

# run ---------------------------------------------------------------------
snapshot_utc <- floor_date(now("UTC"), "30 minutes")
start <- Sys.time()
org_index <- orgIndex()
curr_clients <- org_index[curr_client == TRUE, org_uuid]
kx <- filter(keyIndex(), org_uuid %in% curr_clients)

result <- map_dfr(transpose(kx), inventory_snapshot) |>
  mutate_at(c("product_id", "brand_id", "room_id"), as.character)
result$snapshot_utc <- snapshot_utc

end <- Sys.time()
print(end - start)

# Split into rooms & overall.
result <- select(result, -units_in_stock)

# write/append ------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
dbWriteTable(pg, "leaflogix_rooms_stock_snapshot", result, append = TRUE)
dbd(pg)
