library(dplyr)
library(data.table)
library(jsonlite)
library(httr)
library(lubridate)
library(stringr)
library(purrr)
library(DBI)

#' Aux fun to get the first non-NA value of a vector. Will return NA if all of them are NA.
first_non_na <- function(x) head(c(x[!is.na(x)], x), n = 1)

#' Build Leaflogix inventory snapshot
inventory_snapshot <- function(org, store, auth, consumerkey) {

  # Get snapshot, use retries.
  res <- httr::RETRY(
    "GET",
    "https://publicapi.leaflogix.net/reporting/inventory",
    query = list(includeRoomQuantities = TRUE),
    times = 5,
    quiet = TRUE,
    httr::authenticate(consumerkey, str_remove(auth, "Basic "))
  )
  inventory <- fromJSON(content(res, "text"))

  # Get products.
  res <- httr::RETRY(
    "GET",
    "https://publicapi.leaflogix.net/reporting/products",
    times = 5,
    quiet = TRUE,
    httr::authenticate(consumerkey, str_remove(auth, "Basic "))
  )
  products <- fromJSON(content(res, as = "text"))

  # Try to get products price, using tier data (specially for flowers).
  try({
    products$tier_price <- purrr::map_dbl(products$pricingTierData, function(tier_info) {
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
