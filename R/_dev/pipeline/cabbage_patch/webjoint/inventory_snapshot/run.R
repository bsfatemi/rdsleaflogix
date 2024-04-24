# source ------------------------------------------------------------------
# dir necessary due to the current way we run pipelines as non-installed files
source("inst/pipeline/cabbage_patch/webjoint/inventory_snapshot/init.R", local = TRUE)

box::use(
  DBI[dbExecute, dbWriteTable],
  dplyr[if_else, mutate, mutate_at, select],
  hcaconfig[dbc, dbd, orgIndex],
  lubridate[floor_date, now],
  purrr[map_dfr, transpose]
)

# run ---------------------------------------------------------------------
snapshot_utc <- floor_date(now("UTC"), "30 minutes")
org_index <- orgIndex()
curr_clients <- org_index[curr_client == TRUE, org_uuid]
kx <- keyIndex()[org_uuid %in% curr_clients]

# loops through each row of kx to gather inventory data for each store/facility and timestamps when
# it is queried
result <- map_dfr(transpose(kx), function(k) {
  message(k$short_name, " - ", k$facility)
  email <- k$email
  pw <- k$pw
  domain <- k$domain
  tryCatch(
    {
      get_wj_products(domain, email, pw) |>
        left_join(get_wj_packages(domain, email, pw), by = "product_id", multiple = "all") |>
        mutate(
          org_uuid = !!k$org_uuid, facility = !!k$facility, store_uuid = !!k$store_uuid,
          temp_key = !!domain,
          room_quantity = if_else(is.na(room_quantity), 0, as.numeric(room_quantity))
        )
    },
    error = function(c) {
      message(c$message)
      data.frame()
    }
  )
}) |>
  mutate_at(c("product_id", "brand_id", "room_id"), as.character)
result$snapshot_utc <- snapshot_utc

# Keep needed columns, and replace NAs.
result <- mutate(
  result, product_id = as.character(product_id), sku = if_else(is.na(sku), product_id, sku),
  facility = if_else(is.na(facility), "main", facility)
)

# Split into rooms & overall.
result <- select(result, -quantity)

# write/append ------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
dbWriteTable(pg, "wj_rooms_stock_snapshot", result, append = TRUE)
dbExecute(pg, "ANALYZE wj_rooms_stock_snapshot")
dbd(pg)
