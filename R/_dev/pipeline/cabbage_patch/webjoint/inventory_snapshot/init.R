box::use(
  data.table[as.data.table],
  dplyr[arrange, bind_rows, coalesce, desc, group_by, left_join, mutate, summarise, transmute],
  hcawebjoint[g, get_api_keys, getWJToken],
  jsonlite[fromJSON, toJSON],
  lubridate[as_datetime]
)

# utils ---------------------------------------------------------------
keyIndex <- function() {
  get_api_keys() |>
    transmute(
      org_uuid, store_uuid,
      facility = store_short_name, domain, email, pw = password,
      short_name = org_short_name
    ) |>
    as.data.table()
}

# functions ---------------------------------------------------------------
get_wj_products <- function(domain, email, pw) {
  endpt <- "/products"
  token <- getWJToken(domain, email, pw)
  facility_id <- fromJSON(g(domain, "/facilities", token))$id
  test <- fromJSON(g(
    domain, endpt, token,
    query = list(facilityId = facility_id, range = toJSON(c(0, 99), encoding = "UTF-8"))
  ))
  prods <- data.frame()
  i <- 1
  while (is.data.frame(test) && nrow(test) > 0) {
    prods <- bind_rows(prods, test)
    test <- fromJSON(g(
      domain, endpt, token,
      query = list(
        facilityId = facility_id, range = toJSON(c((i * 100), (i * 100) + 99), encoding = "UTF-8")
      )
    ))
    i <- i + 1
  }
  # Extract products.
  # Get variants data.
  variants <- bind_rows(prods$variant) |>
    # For some columns, keep the last created value.
    arrange(desc(as_datetime(created))) |>
    group_by(productId) |>
    summarise(
      sku = first_non_na(sku),
      unit_of_measure = first_non_na(unit),
      unit_price = median(price, na.rm = TRUE),
      .groups = "drop"
    ) |>
    mutate(sku = coalesce(sku, as.character(productId)))
  left_join(prods, variants, by = c(id = "productId")) |>
    transmute(
      name, quantity, type, lineage, sku,
      product_id = id,
      brand_id = brandId,
      for_sale = forSale,
      created = as_datetime(created),
      brand_name = brand$name,
      strain_name = strain$name,
      strain_indica_sativa_ration = strain$indicaSativaRatio,
      category_name = category$name,
      image = image$url,
      unit_price,
      unit_of_measure
    )
}

first_non_na <- function(vector) {
  head(c(vector[!is.na(vector)], vector), 1)
}

get_wj_packages <- function(domain, email, pw) {
  endpt <- "/packages"
  token <- getWJToken(domain, email, pw)
  facility_id <- fromJSON(g(domain, "/facilities", token))$id
  test <- fromJSON(g(
    domain, endpt, token,
    query = list(facilityId = facility_id, range = toJSON(c(0, 99), encoding = "UTF-8"))
  ))
  packs <- data.frame()
  i <- 1
  while (is.data.frame(test) && nrow(test) > 0) {
    packs <- bind_rows(packs, test)
    test <- fromJSON(g(
      domain, endpt, token,
      query = list(
        facilityId = facility_id,
        range = toJSON(c((i * 100), (i * 100) + 99), encoding = "UTF-8")
      )
    ))
    i <- i + 1
  }
  # Per product summarise the units in stock per room.
  rooms_quantities <- bind_rows(packs$packageQuantities) |>
    transmute(productId, locationId, room_name = location$name, quantity) |>
    group_by(product_id = productId, room_id = locationId, room_name) |>
    summarise(room_quantity = sum(quantity), .groups = "drop")
  # Per product, keep the last updated unit cost that is not NA.
  products_info <- arrange(packs, is.na(costPerItem), desc(as_datetime(received))) |>
    group_by(product_id = productId) |>
    mutate(cost_per_unit = first_non_na(costPerItem)) |>
    group_by(product_id) |>
    summarise(cost_per_unit = head(cost_per_unit, 1), .groups = "drop")
  left_join(products_info, rooms_quantities, by = "product_id", multiple = "all")
}
