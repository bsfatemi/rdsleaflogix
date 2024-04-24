# libs --------------------------------------------------------------------
box::use(
  dplyr[
    arrange, coalesce, desc, distinct, filter, group_by, left_join, select, summarise, transmute
  ],
  hcaflowhub[get_api_keys],
  httr[add_headers, content, RETRY],
  jsonlite[fromJSON],
  lubridate[as_datetime],
  purrr[map_dbl],
  stringr[str_to_lower]
)

# utility functions -------------------------------------------------------
keyIndex <- function() {
  select(
    get_api_keys(),
    org_uuid, apikey, clientid, store_uuid, store_short_name, loc_id
  )
}

# utils -------------------------------------------------------------------
get_org_location <- function(clientid, apikey, loc_id = NULL) {
  res <- try(fromJSON(content(RETRY(
    "GET", "https://api.flowhub.co/v0/clientsLocations",
    add_headers(clientid = clientid, key = apikey)
  ), as = "text", encoding = "UTF-8"))$data)
  if (inherits(res, "try-error")) {
    return(NULL)
  }
  if (!is.null(loc_id)) {
    res <- filter(res, importId == loc_id)
  }
  res
}

inventory_snapshot <- function(org_creds) {
  message("Fetching inventory data for all facilities from ", org_creds$org_uuid)
  lid <- get_org_location(org_creds$clientid, org_creds$apikey, org_creds$loc_id)$locationId
  if (is.null(lid)) {
    message("No locations found")
    return(data.frame())
  }
  # First try the ByRooms endpoint, if it can't be reached (because of credential permissions),
  # then try the general one.
  urls <- c(
    paste0("https://api.flowhub.co/v0/locations/", lid, "/AnalyticsByRooms"),
    paste0("https://api.flowhub.co/v0/locations/", lid, "/Analytics")
  )
  i <- 1
  while (i <= length(urls)) {
    inv <- RETRY(
      "GET", urls[[i]],
      add_headers(clientid = org_creds$clientid, key = org_creds$apikey),
      quiet = TRUE
    )
    message("URL: ", urls[[i]], "\tstatus code ", inv$status_code)
    if (!inv$status_code %in% 200:299 || is.raw(content(inv))) {
      i <- i + 1
      next
    }
    i <- Inf
    inv <- fromJSON(content(inv, as = "text", encoding = "UTF-8"))$data
  }
  if (i == length(urls) + 1 || length(inv) == 0 || nrow(inv) == 0) {
    message("No inventory data returned")
    inv <- data.frame()
  } else {
    if (any(!c("roomId", "roomName") %in% colnames(inv))) {
      inv$roomId <- inv$roomName <- inv$locationName
    }
    inv$tier_price <- map_dbl(inv$weightTierInformation, function(tier_info) {
      if (all(is.na(tier_info))) {
        NA_real_
      }
      else{
        # Let's calculate eighth mean price.
        mean(((tier_info$pricePerUnitInMinorUnits / 100) / tier_info$gramAmount) * 3.5)
      }
    })
    # Note: FlowHub's `productId` and `sku` change between batches, so same product has different
    # IDs and SKUs, so the fix is to assign productName as both of these values.
    # For each product, by its key (productName), get the last updated values for metadata columns.
    products_metadata <- arrange(inv, desc(as_datetime(productUpdatedAt))) |>
      transmute(
        product_id = productName,
        sku = productName,
        # KT 8/16: Calculation below necessary to match skus correctly with unit_price
        # (order_line_list_price) in sales tables ({org}_flowhub_orders_items_in_cart).
        # Need to re-evaluate how discounts are calculated in build_flowhub_order_lines
        unit_price = as.numeric(priceInMinorUnits) / 100,
        unit_price = coalesce(unit_price, tier_price),
        unit_of_measure = productUnitOfMeasure,
        brand,
        brand_id = brand,
        product_name = productName,
        image = productPictureURL,
        raw_category_name = str_to_lower(category),
        raw_product_class = str_to_lower(speciesName),
        # KT 8/16: Calculation below necessary to match how unit_price is done above
        cost_per_unit = as.numeric(costInMinorUnits) / 100
      ) |>
      distinct(product_name, .keep_all = TRUE)
    rooms_stock <- transmute(
      inv,
      units_in_stock = quantity, product_name = productName, facility = locationName,
      room_id = as.character(roomId), room_name = roomName
    ) |>
      group_by(facility, room_id, room_name, product_name) |>
      summarise(units_in_stock = sum(units_in_stock, na.rm = TRUE), .groups = "drop")
    inv <- left_join(rooms_stock, products_metadata, by = "product_name")
    inv$facility <- org_creds$store_short_name
    inv$store_uuid <- org_creds$store_uuid
    inv$org_uuid <- org_creds$org_uuid
    message(nrow(inv), " rows fetched.")
  }
  return(inv)
}
