box::use(
  dplyr[arrange, desc, distinct, filter, group_by, mutate, transmute],
  hcagrowflow[get_access_token, get_products, extract_products],
  lubridate[now, years]
)

inventory_snapshot <- function(kx_row) {
  org_uuid <- kx_row$org_uuid
  store_uuid <- kx_row$store_uuid
  facility <- kx_row$store_short_name
  org_name <- kx_row$org_name
  client_id <- kx_row$client_id
  client_secret <- kx_row$client_secret
  access_token <- get_access_token(client_id, client_secret)

  message(org_name, " - ", facility)
  # Pulling 2 years to grab non-recently updated products
  to <- now()
  from <- to - years(2)
  # Get snapshot.
  query <- get_products(access_token, org_name, from, to)
  products <- extract_products(query)

  # Prioritize active products when filtering duplicates.
  prods <- arrange(products, product_active == TRUE, desc(updated_at)) |>
    transmute(
      org_uuid = !!org_uuid,
      store_uuid = !!store_uuid,
      facility = !!facility,
      room_id = storage_location_object_id,
      product_id,
      product_name = toupper(product_name),
      sku = product_name,
      brand_id = product_brand_id,
      brand = product_brand_name,
      room_name = storage_location_name,
      units_in_stock = as.numeric(current_qty),
      image,
      unit_price = as.numeric(product_sales_price / 100),
      unit_of_measure = product_unit_weight_unit_of_measure,
      raw_category_name = product_product_category_name,
      raw_product_class = product_name
    ) |>
    filter(!is.na(sku)) |>
    group_by(room_id, sku) |>
    mutate(units_in_stock = sum(units_in_stock, na.rm = TRUE)) |>
    distinct(room_id, sku, .keep_all = TRUE)
}
