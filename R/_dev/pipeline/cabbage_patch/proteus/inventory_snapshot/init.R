box::use(
  dplyr[arrange, as_tibble, distinct, group_by, left_join, mutate, transmute, ungroup],
  fs[path_ext, path_package],
  hcaconfig[get_org_stores],
  hcaproteus[get_proteus_products],
  purrr[map_dfr],
  RJSONIO[fromJSON],
  yaml[read_yaml]
)

# utils -------------------------------------------------------------------
# grabs all credentials for proteus clients in raw form
..fp <- function(f) {
  read_yaml(path_package("hcaproteus", "extdata", path_ext(f), f))
}

keyIndex <- function() {
  # Get file credentials.
  credentials <- ..fp("src-proteus.yml")$proteus_uuid$creds$clients
  credentials <- map_dfr(credentials, function(credential) {
    data.frame(org_uuid = names(credential), as_tibble((credential[[1]])))
  }) |>
    left_join(get_org_stores(), by = "org_uuid")
}

#' Build proteus inventory snapshot
inventory_snapshot <- function(kx_row) {
  org_uuid <- kx_row$org_uuid
  store_uuid <- kx_row$store_uuid
  facility <- kx_row$facility
  apikey <- list(apikey = list(key = kx_row$key, clientname = kx_row$clientname))
  message(org_uuid, " - ", facility)
  # Get snapshot.
  query <- get_proteus_products(apikey)
  inventory <- map_dfr(unlist(query), ~ map_dfr(fromJSON(.x), function(item) {
    # Some columns fixing.
    item$num_in_stock <- as.numeric(item$num_in_stock)
    item$parent_id <- as.character(item$parent_id)
    item
  })) |>
    distinct()
  # Prioritize active products when filtering duplicates.
  arrange(inventory, active != 1) |>
    transmute(
      org_uuid = !!org_uuid,
      store_uuid = !!store_uuid,
      facility = !!facility,
      room_id = !!facility,
      product_id,
      sku,
      product_name = name,
      brand_id = brand_name,
      brand = brand_name,
      room_name = !!facility,
      units_in_stock = as.numeric(num_in_stock),
      image,
      unit_price = as.numeric(price),
      unit_of_measure = unit,
      raw_category_name = category,
      raw_product_class = name
    ) |>
    # Summarise duplicates by SKU.
    group_by(sku) |>
    mutate(
      units_in_stock = sum(units_in_stock, na.rm = TRUE),
      unit_price = mean(unit_price, na.rm = TRUE)
    ) |>
    ungroup() |>
    distinct(sku, .keep_all = TRUE)
}
