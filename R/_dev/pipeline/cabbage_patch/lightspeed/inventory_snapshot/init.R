box::use(
  dplyr[bind_rows, case_when, group_by, left_join, mutate, summarise, transmute],
  hcalightspeed[get_access_token, get_account],
  jsonlite[fromJSON],
  lubridate[today, years],
  purrr[map_dfr],
  stats[setNames]
)

# utils -------------------------------------------------------------------
#' Build lightspeed inventory snapshot
inventory_snapshot <- function(kx_row) {
  message(kx_row$org_short_name, " - ", kx_row$store_short_name)
  access_token <- get_access_token(kx_row$refresh_token)
  account_id <- get_account(access_token)$Account$accountID
  # Query all of the data.
  start_date <- today() - years(420)
  # Query items, with shops stock data.
  items_raw <- hcalightspeed:::get_lightspeed(
    "Item", c("Category", "Images", "ItemPrices", "ItemShops", "Manufacturer"), account_id,
    access_token, kx_row$refresh_token, start_date
  )
  items <- map_dfr(items_raw, ~ fromJSON(.x)$Item)
  shops_stock <- bind_rows(items$ItemShops$ItemShop) |>
    mutate(units_in_stock = as.numeric(qoh)) |>
    group_by(itemID, shopID) |>
    summarise(units_in_stock = sum(units_in_stock), .groups = "drop")
  items_price <- setNames(items$Prices$ItemPrice, items$itemID) |>
    bind_rows(.id = "itemID") |>
    mutate(amount = as.numeric(amount)) |>
    group_by(itemID) |>
    summarise(unit_price = mean(amount), .groups = "drop")
  # Query shops.
  shops_raw <- hcalightspeed:::get_lightspeed(
    "Shop", c(), account_id, access_token, kx_row$refresh_token, start_date, sort = "shopID"
  )
  shops <- map_dfr(shops_raw, ~ fromJSON(.x)$Shop) |>
    transmute(shopID, room_name = toupper(name))
  # Join all the data.
  left_join(items, items_price, by = "itemID") |>
    left_join(shops_stock, by = "itemID") |>
    left_join(shops, by = "shopID") |>
    transmute(
      org_uuid = !!kx_row$org_uuid,
      store_uuid = !!kx_row$store_uuid,
      facility = !!kx_row$org_short_name,
      product_id = itemID,
      sku = systemSku,
      product_name = toupper(description),
      brand_id = manufacturerID,
      brand = toupper(Manufacturer$name),
      room_id = shopID,
      room_name,
      units_in_stock,
      image = NA_character_,
      unit_price,
      unit_of_measure = NA_character_,
      cost_per_unit = as.numeric(defaultCost),
      raw_product_class = case_when(
        grepl("Indica", Category$fullPathName, ignore.case = TRUE) ~ "indica",
        grepl("Sativa", Category$fullPathName, ignore.case = TRUE) ~ "sativa",
        grepl("Hybrid", Category$fullPathName, ignore.case = TRUE) ~ "hybrid"
      ),
      raw_category_name = tolower(gsub("/.*", "", Category$fullPathName))
    )
}
