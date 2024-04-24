# source ------------------------------------------------------------------
# dir necessary due to the current way we run pipelines as non-installed files
source("inst/pipeline/cabbage_patch/akerna/inventory_snapshot/init.R", local = TRUE)

box::use(
  DBI[dbExecute, dbWriteTable],
  hcaconfig[dbc, dbd, orgIndex],
  lubridate[floor_date, now],
  purrr[map_dfr, transpose]
)

# run ---------------------------------------------------------------------
snapshot_utc <- floor_date(now("UTC"), "30 minutes")
org_index <- orgIndex()
curr_clients <- org_index[curr_client == TRUE, org_uuid]
kx <- filter(keyIndex(org_index), org_uuid %in% curr_clients)

result <- map_dfr(transpose(kx), function(org_creds) {
  inv_snaphot <- try(inventory_snapshot(org_creds))
  if (inherits(inv_snaphot, "try-error")) {
    return(NULL)
  }
  inv_snaphot
})

if (nrow(result) == 0 && nrow(kx) > 0) {
  # If it failed for all orgs, then fail. TODO: We should message each failing org anyways.
  stop(
    "Akerna inventory could not be obtained for any single org; #orgs: ",
    length(unique(kx$org_uuid))
  )
}
result$snapshot_utc <- snapshot_utc

# write/append ------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
dbWriteTable(pg, "akerna_rooms_stock_snapshot", result, append = TRUE)
dbExecute(pg, "ANALYZE akerna_rooms_stock_snapshot")
dbd(pg)
