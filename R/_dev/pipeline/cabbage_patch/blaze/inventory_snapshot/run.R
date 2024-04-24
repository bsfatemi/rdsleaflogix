# source ------------------------------------------------------------------
source("inst/pipeline/cabbage_patch/blaze/inventory_snapshot/init.R", local = TRUE)
source("inst/pipeline/cabbage_patch/blaze/inventory_snapshot/src/anonymize_demoorg.R", local = TRUE)

box::use(
  data.table[rbindlist, setcolorder],
  dplyr[bind_rows, filter],
  DBI[dbExecute, dbWriteTable],
  dplyr[
    `%>%`, arrange, bind_rows, desc, distinct, if_else, left_join, mutate, select, tibble
  ],
  hcablaze[extractBlaze, getBlaze],
  hcaconfig[dbc, dbd, orgIndex],
  jsonlite[fromJSON],
  lubridate[floor_date, now],
  purrr[compact, map, map_dbl, map_depth, map_dfr, safely, transpose],
  stringr[str_glue],
  tidyr[hoist, unnest_wider],
  stats[setNames],
  yaml[read_yaml]
)

# run bind and clean ------------------------------------------------------
snapshot_utc <- floor_date(now("UTC"), "30 minutes")
start <- Sys.time()
org_index <- orgIndex()
curr_clients <- org_index[curr_client == TRUE, org_uuid]
kx <- keyIndex(org_index)[org_uuid %in% curr_clients]

# Used for batches query.
stop_utc <- now(tzone = "UTC")
start_utc <- stop_utc - months(1)

process <- function(k) {
  print(str_glue("Processing... {k$org} {k$facility}"))

  # Get inventory rooms.
  inv_rooms <- getBlaze("/store/inventory/inventories", "start", k$apikey) %>%
    map(~ fromJSON(.x, simplifyVector = FALSE)) %>%
    map(`[[`, "values") %>%
    unlist(recursive = FALSE) %>%
    map(compact) %>%
    tibble(list = .) %>%
    unnest_wider(col = "list") %>%
    select(inventoryId = id, inventory_room = name)

  # Just taking the internals here in hcablaze::extractBlaze and modifying slightly
  df <- getBlaze("/products", "skip", k$apikey) %>%
    map(~ fromJSON(.x, simplifyVector = FALSE)) %>%
    map(`[[`, "values") %>%
    unlist(recursive = FALSE) %>%
    map(compact) %>%
    tibble(list = .) %>%
    unnest_wider(col = "list") %>%
    hoist(category,
      categoryId = "id", "category", category_name = "name", "unitType",
      "cannabis", "cannabisType", "wmCategory",
    ) %>%
    hoist(assets, image = list(1, "publicURL"))
  quantities <- map_dfr(transpose(df), function(product) {
    res <- bind_rows(product$quantities)
    res$product_id <- product$id
    res
  }) |>
    left_join(inv_rooms, by = "inventoryId") |>
    select(product_id, inventoryId, inventory_room, quantity)
  df <- left_join(df, quantities, by = c(id = "product_id"), multiple = "all") |>
    mutate(quantity = if_else(is.na(quantity), 0, quantity))

  # Fix when Blaze stops sending category name as "category", and instead sends it as "name".
  if (is.list(df$category) && "category_name" %in% colnames(df)) {
    df$category <- df$category_name
  }
  df <- select(
    df, id, inventoryId, inventory_room, active, sku, instock, discountable, name, brandId, byGram,
    byPrepackage, flowerType, unitPrice, weightPerUnit, unitValue, thc, cbn, cbd, cbda, thca,
    description, quantity, image, categoryId, unitType, cannabis, cannabisType, wmCategory, category
  )

  brands <- extractBlaze(
    getBlaze("/store/inventory/brands", "start", k$apikey, term = ""),
    ept = "brands"
  ) %>%
    distinct(brandId = id, brand = name)
  df <- left_join(df, brands, by = "brandId")

  # Use batches to obtain product unit cost.
  batches <- extractBlaze(
    getBlaze(
      "/store/batches/dates", "start", k$apikey,
      startDate = format(as.numeric(start_utc) * 1000, scientific = FALSE),
      endDate = format(as.numeric(stop_utc) * 1000, scientific = FALSE)
    ),
    ept = "batches"
  )
  if (nrow(batches) > 1 && all(c("product_id", "cost_per_unit") %in% colnames(batches))) {
    # Per product, keep the last updated `cost_per_unit` that is not NA.
    batches <- arrange(batches, is.na(cost_per_unit), desc(modified)) |>
      distinct(product_id, .keep_all = TRUE) |>
      select(product_id, cost_per_unit)
    df <- left_join(df, batches, by = c("id" = "product_id"))
  }

  df$org_uuid <- k$org_uuid
  df$store_uuid <- k$store_uuid
  df$facility <- k$facility

  # Anonymization for DemoOrg.
  if (nrow(df) > 0 && k$org == "demoorg") {
    df <- anonymize_demoorg(df)
  }

  return(df)
}
safe_process <- safely(process, quiet = FALSE)
prods <- map(transpose(kx), safe_process) %>%
  map(`[[`, "result") %>%
  compact() %>%
  bind_rows()

end <- Sys.time()
print(end - start)

prods$snapshot_utc <- snapshot_utc

# write out ---------------------------------------------------------------
cn <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
dbWriteTable(cn, "blaze_rooms_stock_snapshot", prods, append = TRUE)
dbExecute(cn, "ANALYZE blaze_rooms_stock_snapshot")
dbd(cn)
