# Libraries imports.
box::use(
  dplyr[
    arrange, bind_rows, desc, distinct, group_by, inner_join, left_join, mutate, select, summarise,
    transmute
  ],
  hcaposabit[extract_inventories, extract_manifest_items, get_inventories, get_manifest_items],
  lubridate[as_datetime]
)

get_stock_snapshot <- function(curr_client) {
  # Get and extract the manifest_items endpoint for cost_per_unit
  manifest_items <- extract_manifest_items(get_manifest_items(curr_client$api_token))
  manifest_unit_cost <- arrange(manifest_items, desc(as_datetime(updated_at))) |>
    distinct(inventory_id, .keep_all = TRUE) |>
    transmute(id = inventory_id, cost_per_unit = cost_per_unit / 100)
  # Get and extract the products/inventory endpoint.
  inventories <- extract_inventories(get_inventories(curr_client$api_token))
  inventory <- left_join(inventories$inventories, manifest_unit_cost, by = "id") |>
    transmute(
      product_id = as.character(product_id), sku, product_name = toupper(name), brand_id = brand,
      brand = toupper(brand), image = first_image, unit_price = price / 100,
      unit_of_measure = gsub(".* ", "", unit), cost_per_unit,
      raw_category_name = tolower(category), raw_product_class = tolower(flower_type)
    ) |>
    distinct(product_id, .keep_all = TRUE)
  # Inventory in rooms.
  inventories_rooms <- distinct(
    inventories$inventories,
    inventory_id = as.character(id), product_id = as.character(product_id)
  ) |>
    inner_join(inventories$inventories_rooms, by = "inventory_id") |>
    group_by(product_id, room_id = room, room_name = room) |>
    summarise(units_in_stock = sum(as.numeric(room_qty), na.rm = TRUE), .groups = "drop")
  # There are inventory products with no rooms assigned. So, let's calculate these remain.
  inventory_in_rooms <- group_by(inventories_rooms, product_id = as.character(product_id)) |>
    summarise(units_in_rooms = sum(units_in_stock), .groups = "drop")
  inventories_total <- group_by(inventories$inventories, product_id = as.character(product_id)) |>
    summarise(units_in_stock = sum(as.numeric(quantity_on_hand), na.rm = TRUE), .groups = "drop") |>
    left_join(inventory_in_rooms, by = "product_id") |>
    mutate(units_in_stock = units_in_stock - pmax(0, units_in_rooms, na.rm = TRUE)) |>
    select(product_id, units_in_stock)
  # Join all info.
  left_join(
    inventory, bind_rows(inventories_total, inventories_rooms),
    by = "product_id"
  ) |>
    mutate(
      org_uuid = curr_client$org_uuid, store_uuid = curr_client$store_uuid,
      facility = curr_client$store_short_name
    )
}
