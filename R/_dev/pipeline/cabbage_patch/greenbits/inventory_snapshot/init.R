box::use(
  dplyr[bind_cols, case_when, coalesce, left_join, rename_all, transmute],
  hcagreenbits[get_gb],
  jsonlite[fromJSON],
  purrr[map_chr, map_dfr]
)

# utils -------------------------------------------------------------------
#' Build Greenbits inventory snapshot
inventory_snapshot <- function(kx_row) {
  facility <- kx_row$store_short_name
  message(kx_row$org_short_name, " - ", facility)
  # Get inventory.
  inventory <- get_gb("products", kx_row$company_id, kx_row$client_id, kx_row$token, limit = 1000)
  inventory <- map_dfr(inventory, ~ fromJSON(.x)$products)
  inventory <- bind_cols(inventory, rename_all(inventory$quantity, ~ paste0("quantity_", .x))) |>
    bind_cols(rename_all(inventory$weight, ~ paste0("weight_", .x)))
  inventory$image <- map_chr(inventory$images, ~ ifelse(is.null(.x$url[[1]]), "", .x$url[[1]]))
  # Get brands.
  brands <- get_gb("brands", kx_row$company_id, kx_row$client_id, kx_row$token, limit = 1000)
  brands <- map_dfr(brands, ~ fromJSON(.x)$brands) |>
    transmute(brand_id = id, brand = name)
  # Get product_types.
  product_types <- get_gb("product_types", kx_row$company_id, kx_row$client_id, kx_row$token)
  product_types <- map_dfr(product_types, ~ fromJSON(.x)$product_types) |>
    transmute(product_type_id = id, raw_category_name = name)

  # Parse the rooms inventories.
  left_join(inventory, product_types, by = "product_type_id") |>
    transmute(
      org_uuid = !!kx_row$org_uuid,
      store_uuid = !!kx_row$store_uuid,
      facility = !!facility,
      room_id = !!facility,
      product_id = id,
      sku = coalesce(sku, latest_sku),
      product_name = name,
      brand_id = brand_id,
      room_name = !!facility,
      units_in_stock = as.numeric(quantity_value),
      image,
      unit_price = sell_price / 100,
      unit_of_measure = case_when(
        weight_unit == 0 ~ "GRAM",
        weight_unit == 1 ~ "OUNCE",
        weight_unit == 2 ~ "POUND",
        weight_unit == 3 ~ "MILLIGRAM",
        weight_unit == 4 ~ "KILOGRAM",
        weight_unit == 5 ~ "EACH"
      ),
      # cost_per_unit,
      raw_category_name,
      raw_product_class = case_when(
        flower_type == 0 ~ "Indica",
        flower_type == 1 ~ "Sativa",
        flower_type == 2 ~ "Hybrid",
        flower_type == 3 ~ "Indica Hybrid",
        flower_type == 4 ~ "Sativa Hybrid"
      )
    ) |>
    left_join(brands, by = "brand_id")
}
