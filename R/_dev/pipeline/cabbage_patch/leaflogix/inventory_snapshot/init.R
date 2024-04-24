box::use(
  data.table[as.data.table, rbindlist],
  dplyr[
    arrange, bind_rows, coalesce, desc, filter, group_by, if_else, left_join, mutate, select,
    summarise, transmute
  ],
  fs[path_ext, path_package],
  hcaleaflogix[get_api_keys],
  httr[authenticate, content, RETRY],
  jsonlite[fromJSON],
  lubridate[as_datetime, floor_date, now],
  purrr[map_dbl],
  stringr[str_remove],
  yaml[read_yaml]
)

# utils -------------------------------------------------------------------
# grabs all credentials for leaflogix clients in raw form
..fp <- function(f) {
  read_yaml(path_package("hcaleaflogix", "extdata", path_ext(f), f))
}


keyIndex <- function() {
  # Get database file credentials.
  transmute(
    get_api_keys(),
    org_uuid, store_uuid,
    facility = store_short_name, auth = paste("Basic", auth), consumerkey,
    short_name = org_short_name
  )
}

#' Aux fun to get the first non-NA value of a vector. Will return NA if all of them are NA.
first_non_na <- function(x) head(c(x[!is.na(x)], x), n = 1)

#' Build Leaflogix inventory snapshot
inventory_snapshot <- function(kx_row) {
  org_uuid <- kx_row$org_uuid
  short_name <- kx_row$short_name
  store_uuid <- kx_row$store_uuid
  facility <- kx_row$facility
  consumerkey <- kx_row$consumerkey
  auth <- kx_row$auth
  message(short_name, " - ", facility)

  # Get snapshot, use retries.
  res <- RETRY(
    "GET",
    "https://publicapi.leaflogix.net/reporting/inventory",
    query = list(includeRoomQuantities = TRUE),
    times = 5,
    quiet = TRUE,
    authenticate(consumerkey, str_remove(auth, "Basic "))
  )
  inventory <- try(fromJSON(content(res, as = "text")))
  if (res$status_code != 200 || inherits(inventory, "try-error") || !is.data.frame(inventory)) {
    message("No response from org_uuid ", org_uuid, " facility: ", facility)
    return(data.frame())
  }
  # Get products.
  res <- RETRY(
    "GET",
    "https://publicapi.leaflogix.net/reporting/products",
    times = 5,
    quiet = TRUE,
    authenticate(consumerkey, str_remove(auth, "Basic "))
  )
  products <- fromJSON(content(res, as = "text"))
  # Try to get products price, using tier data (specially for flowers).
  try({
    products$tier_price <- map_dbl(products$pricingTierData, function(tier_info) {
      suppressWarnings(mean(tier_info$price)) # All rows seem to be at gram price.
    })
    products <- mutate(products, price = coalesce(price, tier_price))
    inventory <- left_join(
      inventory, select(products, productId, unit_price = price), by = "productId"
    ) |>
      mutate(unitPrice = pmax(unitPrice, unit_price, na.rm = TRUE))
  }, silent = TRUE)
  # Get the products that have no stock (not returned by the inventory endpoint).
  no_stock_prods <- filter(products, !sku %in% inventory$sku) |>
    transmute(
      productId, quantityAvailable = 0, sku, unitPrice = price,
      unitWeightUnit = defaultUnit, category, brandId, brandName, productName, imageUrl, unitCost,
      strainType
    )
  # Add no stock products to inventory.
  inventory <- bind_rows(inventory, no_stock_prods)

  # Parse the rooms inventories.
  room_quantities <- map_dfr(transpose(inventory), function(product) {
    res <- bind_rows(product$roomQuantities)
    res$sku <- product$sku
    res
  }) |>
    group_by(sku, room_id = roomId, room_name = room) |>
    summarise(room_quantity = sum(quantityAvailable), .groups = "drop")
  # For each product, if there are multiple rows that we need to keep just one, then keep the last
  # updated one.
  arrange(inventory, desc(as_datetime(lastModifiedDateUtc))) |>
    group_by(org_uuid = !!org_uuid, store_uuid = !!store_uuid, facility = !!facility, sku) |>
    summarise(
      units_in_stock = sum(quantityAvailable),
      product_id = first_non_na(productId),
      unit_price = first_non_na(unitPrice),
      unit_of_measure = first_non_na(unitWeightUnit), # not sure
      raw_category_name = first_non_na(category),
      brand_id = first_non_na(brandId),
      brand = first_non_na(brandName),
      product_name = first_non_na(productName),
      image = first_non_na(imageUrl),
      cost_per_unit = first_non_na(unitCost),
      track_package_label = NA_character_,
      raw_product_class = first_non_na(strainType),
      .groups = "drop"
    ) |>
    left_join(room_quantities, by = "sku") |>
    mutate(room_quantity = if_else(is.na(room_quantity), 0, room_quantity))
}
