# source ------------------------------------------------------------------
source(
  "inst/pipeline/cabbage_patch/treez2-clients/inventory_snapshot/src/get_trz_snapshot.R",
  local = TRUE
)

box::use(
  DBI[dbExecute, dbWriteTable],
  dplyr[coalesce, distinct, filter, if_else, left_join, mutate, select_at],
  hcaconfig[dbc, dbd, orgIndex],
  hcatreez[extract_stock, extractTreez, get_stock, getHCAKey, getProducts],
  lubridate[floor_date, now, today, years],
  purrr[map_dfr, transpose]
)

# run bind and clean ------------------------------------------------------
snapshot_utc <- floor_date(now("UTC"), "30 minutes")
start <- Sys.time()
org_index <- orgIndex()
curr_clients <- org_index[curr_client == TRUE, org_uuid]
kx <- keyIndex(org_index)[org_uuid %in% curr_clients]
client_id <- getHCAKey()
eob_utc <- today() - years(2)

# loops through each row of kx to gather inventory data for each store/facility and timestamps when
# it is queried
rooms_inv <- map_dfr(transpose(kx), function(k) {
  key <- k$apikey
  disp <- k$disp_name
  print(paste(k$short_name, k$facility, disp, sep = " - "))
  tryCatch(
    {
      stock <- extract_stock(get_stock(key, client_id, disp, eob_utc))
      stock$room_quantity <- stock$sellable_quantity
      if ("available_units" %in% colnames(stock)) {
        stock$room_quantity <- stock$available_units
      }
      products <- extractTreez(getProducts(key, client_id, disp, eob_utc), "products")
      products <- transmute(
        products, product_id, category_type, name, price_sell, brand, size, uom, product_status,
        classification, primary_image, product_barcodes
      )
      # I've seen a really few cases where we get duplicate `product_id`s, this seems to be Treez's
      # issue, so we can keep the `last_updated_at` row instead of summarizing.
      if (length(products$product_id) != length(unique(products$product_id))) {
        message(
          "Removing ", length(products$product_id) - length(unique(products$product_id)),
          " duplicate rows."
        )
        products <- distinct(products, product_id, .keep_all = TRUE)
      }
      left_join(stock, products, by = "product_id") |>
        mutate(
          room_id = toupper(location_name), inventory_room = room_id,
          room_quantity, org_uuid = k$org_uuid, store_uuid = k$store_uuid,
          facility = k$facility,
          # There are some products found in stock, but not in products endpoint.
          category_type = coalesce(category_type, product_type),
          name = coalesce(name, product_name),
          brand = coalesce(brand, product_brand),
          uom = coalesce(uom, product_uom),
          product_status = coalesce(product_status, "ACTIVE")
        )
    },
    error = function(c) {
      message(c$message)
      data.frame()
    }
  )
})
rooms_inv$snapshot_utc <- snapshot_utc
rooms_inv <- grab_product_sku(rooms_inv)

endcols <- c(
  "snapshot_utc", "org_uuid", "store_uuid", "facility", "room_id", "product_id", "sku", "name",
  "brand", "inventory_room", "room_quantity", "primary_image", "price_sell", "uom",
  "category_type", "classification",
  "last_updated_at", "size", "product_status"
)

rooms_inv <- mutate(rooms_inv, sku = if_else(is.na(sku), product_id, sku)) |>
  select_at(endcols)

end <- Sys.time()
print(end - start)

# write/append ------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
dbWriteTable(pg, "treez_rooms_stock_snapshot", rooms_inv, append = TRUE)
dbExecute(pg, "ANALYZE treez_rooms_stock_snapshot")
dbd(pg)
