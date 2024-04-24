# Libraries imports.
box::use(
  dplyr[arrange, bind_rows, desc, distinct, group_by, inner_join, mutate, transmute, ungroup],
  hcameadow[extract_products, get_products],
  jsonlite[fromJSON],
  lubridate[as_datetime]
)

get_stock_snapshot <- function(curr_client) {
  # Get and extract the products/inventory endpoint.
  products <- extract_products(get_products(curr_client$consumer_key, curr_client$client_key))
  inventory <- transmute(
    products$inventory,
    product_id = as.character(product_id), room_id = as.character(inventory_location_id),
    room_name = name, units_in_stock = as.numeric(amount)
  ) |>
    group_by(product_id, room_id) |>
    mutate(units_in_stock = sum(units_in_stock, na.rm = TRUE)) |>
    ungroup() |>
    distinct(product_id, room_id, .keep_all = TRUE)
  products <- products$products
  # Create the stock snapshot data.
  transmute(
    products,
    product_id = as.character(id),
    sku = product_id,
    product_name = toupper(name),
    brand_id = as.character(brand_id),
    brand = toupper(brand_name),
    image,
    unit_price,
    unit_of_measure = toupper(unit),
    cost_per_unit = NA_real_, # TODO: Check if this column makes sense moving_average_cost_per_unit.
    raw_category_name = primary_category_name,
    raw_product_class = strain_type
  ) |>
    inner_join(inventory, by = "product_id", multiple = "all") |>
    mutate(
      org_uuid = curr_client$org_uuid, store_uuid = curr_client$store_uuid,
      facility = curr_client$store_short_name
    )
}
