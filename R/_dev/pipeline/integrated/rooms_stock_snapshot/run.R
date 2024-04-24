# Consolidated Rooms Stock Snapshot
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

# source ------------------------------------------------------------------
# Purge the box module cache, so the app can be reloaded without restarting the R session.
rm(list = ls(box:::loaded_mods), envir = box:::loaded_mods)

# Allow absolute module imports (relative to the project root).
options(box.path = getwd())

box::use(
  data.table[as.data.table],
  DBI[dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[
    bind_rows, case_when, coalesce, filter, group_by, if_else, left_join, mutate, mutate_at,
    select, summarise
  ],
  hcaconfig[dbc, dbd],
  pipelinetools[
    clean_product_brands, clean_product_categories, clean_product_classification,
    get_categories_mapping, str_clean
  ],
  stringr[str_to_lower]
)

box::use(
  inst/pipeline/integrated/population/src/build_category3[build_category3]
)

# Read --------------------------------------------------------------------
# cfg to connect in `hcaconfig::dbc`, "prod2" by default if "HCA_ENV" not set.
(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))
pg <- dbc(db_cfg, "cabbage_patch")

## blaze ------------------------------------------------------------------
blaze_ss <- dbGetQuery(
  pg,
  'SELECT
    \'blaze\' as pos,
    snapshot_utc,
    org_uuid,
    store_uuid,
    facility,
    "inventoryId" as room_id,
    id as product_id,
    sku,
    name as product_name,
    "brandId" as brand_id,
    brand,
    inventory_room as room_name,
    quantity as units_in_stock,
    image,
    "unitPrice" as unit_price,
    "unitType" as unit_of_measure,
    cost_per_unit,
    category as raw_category_name,
    "flowerType" as raw_product_class
   FROM blaze_rooms_stock_snapshot
   WHERE snapshot_utc = (SELECT MAX(snapshot_utc) FROM blaze_rooms_stock_snapshot)'
)

## treez ------------------------------------------------------------------
treez_ss <- dbGetQuery(
  pg,
  "SELECT
    'treez' as pos,
    snapshot_utc,
    org_uuid,
    store_uuid,
    facility,
    room_id,
    product_id,
    sku,
    name as product_name,
    brand as brand_id,
    brand,
    inventory_room as room_name,
    room_quantity as units_in_stock,
    primary_image as image,
    price_sell as unit_price,
    uom as unit_of_measure,
    category_type as raw_category_name,
    classification as raw_product_class
  FROM treez_rooms_stock_snapshot
  WHERE snapshot_utc = (SELECT MAX(snapshot_utc) FROM treez_rooms_stock_snapshot)"
)

## webjoint ----------------------------------------------------------------
webjoint_ss <- dbGetQuery(
  pg,
  "SELECT
    'webjoint' as pos,
    snapshot_utc,
    org_uuid,
    store_uuid,
    facility,
    room_id,
    product_id,
    sku,
    name as product_name,
    brand_name as brand_id,
    brand_name as brand,
    room_name,
    room_quantity as units_in_stock,
    image,
    unit_price,
    unit_of_measure,
    cost_per_unit,
    category_name as raw_category_name,
    lineage as raw_product_class
  FROM wj_rooms_stock_snapshot
  WHERE snapshot_utc = (SELECT MAX(snapshot_utc) FROM wj_rooms_stock_snapshot);"
)

## leaflogix ---------------------------------------------------------------
leaflogix_ss <- dbGetQuery(
  pg,
  "SELECT
    'leaflogix' as pos,
    snapshot_utc,
    org_uuid,
    store_uuid,
    facility,
    room_id,
    product_id,
    sku,
    product_name,
    brand_id,
    brand,
    room_name,
    room_quantity as units_in_stock,
    image,
    unit_price,
    unit_of_measure,
    cost_per_unit,
    raw_category_name,
    raw_product_class
  FROM leaflogix_rooms_stock_snapshot
  WHERE snapshot_utc = (SELECT MAX(snapshot_utc) FROM leaflogix_rooms_stock_snapshot);"
)

## flowhub -----------------------------------------------------------------
flowhub_ss <- dbGetQuery(
  pg,
  "SELECT
    'flowhub' as pos,
    snapshot_utc,
    org_uuid,
    store_uuid,
    facility,
    room_id,
    product_id,
    sku,
    product_name,
    brand_id,
    brand,
    room_name,
    units_in_stock,
    image,
    unit_price,
    unit_of_measure,
    cost_per_unit,
    raw_category_name,
    raw_product_class
  FROM flowhub_rooms_stock_snapshot
  WHERE snapshot_utc = (SELECT MAX(snapshot_utc) FROM flowhub_rooms_stock_snapshot);"
)

## akerna ------------------------------------------------------------------
akerna_ss <- dbGetQuery(
  pg,
  "SELECT
    'akerna' as pos,
    snapshot_utc,
    org_uuid,
    store_uuid,
    facility,
    room_id,
    product_id,
    sku,
    product_name,
    brand_id,
    brand,
    room_name,
    units_in_stock,
    image,
    unit_price,
    unit_of_measure,
    raw_category_name,
    raw_product_class
  FROM akerna_rooms_stock_snapshot
  WHERE snapshot_utc = (SELECT MAX(snapshot_utc) FROM akerna_rooms_stock_snapshot);"
)

## greenbits ---------------------------------------------------------------
greenbits_ss <- dbGetQuery(
  pg,
  "SELECT
    'greenbits' as pos,
    snapshot_utc,
    org_uuid,
    store_uuid,
    facility,
    room_id,
    product_id,
    sku,
    product_name,
    brand_id,
    brand,
    room_name,
    units_in_stock,
    image,
    unit_price,
    unit_of_measure,
    raw_category_name,
    raw_product_class
  FROM greenbits_rooms_stock_snapshot
  WHERE snapshot_utc = (SELECT MAX(snapshot_utc) FROM greenbits_rooms_stock_snapshot);"
)

## lightspeed --------------------------------------------------------------
lightspeed_ss <- dbGetQuery(
  pg,
  "SELECT
    'lightspeed' as pos,
    snapshot_utc,
    org_uuid,
    store_uuid,
    facility,
    room_id,
    product_id,
    sku,
    product_name,
    brand_id,
    brand,
    room_name,
    units_in_stock,
    image,
    unit_price,
    unit_of_measure,
    cost_per_unit,
    raw_category_name,
    raw_product_class
  FROM lightspeed_rooms_stock_snapshot
  WHERE snapshot_utc = (SELECT MAX(snapshot_utc) FROM lightspeed_rooms_stock_snapshot);"
)

## meadow ------------------------------------------------------------------
meadow_ss <- dbGetQuery(
  pg,
  "SELECT
    'meadow' as pos,
    snapshot_utc,
    org_uuid,
    store_uuid,
    facility,
    room_id,
    product_id,
    sku,
    product_name,
    brand_id,
    brand,
    room_name,
    units_in_stock,
    image,
    unit_price,
    unit_of_measure,
    cost_per_unit,
    raw_category_name,
    raw_product_class
  FROM meadow_rooms_stock_snapshot
  WHERE snapshot_utc = (SELECT MAX(snapshot_utc) FROM meadow_rooms_stock_snapshot);"
)

## posabit -----------------------------------------------------------------
posabit_ss <- dbGetQuery(
  pg,
  "SELECT
    'posabit' as pos,
    snapshot_utc,
    org_uuid,
    store_uuid,
    facility,
    room_id,
    product_id,
    sku,
    product_name,
    brand_id,
    brand,
    room_name,
    units_in_stock,
    image,
    unit_price,
    unit_of_measure,
    cost_per_unit,
    raw_category_name,
    raw_product_class
  FROM posabit_rooms_stock_snapshot
  WHERE snapshot_utc = (SELECT MAX(snapshot_utc) FROM posabit_rooms_stock_snapshot);"
)

## proteus -----------------------------------------------------------------
proteus_ss <- dbGetQuery(
  pg,
  "SELECT
    'proteus' as pos,
    snapshot_utc,
    org_uuid,
    store_uuid,
    facility,
    room_id,
    product_id,
    sku,
    product_name,
    brand_id,
    brand,
    room_name,
    units_in_stock,
    image,
    unit_price,
    unit_of_measure,
    raw_category_name,
    raw_product_class
  FROM proteus_rooms_stock_snapshot
  WHERE snapshot_utc = (SELECT MAX(snapshot_utc) FROM proteus_rooms_stock_snapshot);"
)

## growflow -----------------------------------------------------------------
growflow_ss <- dbGetQuery(
  pg,
  "SELECT
    'growflow' as pos,
    snapshot_utc,
    org_uuid,
    store_uuid,
    facility,
    room_id,
    product_id,
    sku,
    product_name,
    brand_id,
    brand,
    room_name,
    units_in_stock,
    image,
    unit_price,
    unit_of_measure,
    raw_category_name,
    raw_product_class
  FROM growflow_rooms_stock_snapshot
  WHERE snapshot_utc = (SELECT MAX(snapshot_utc) FROM growflow_rooms_stock_snapshot);"
)

# Disconnect database.
dbd(pg)

# Build -------------------------------------------------------------------

# bind all tables
population_ss <- bind_rows(
  blaze_ss, treez_ss, webjoint_ss, leaflogix_ss, flowhub_ss, akerna_ss, greenbits_ss, lightspeed_ss,
  meadow_ss, posabit_ss, proteus_ss, growflow_ss
)
rm(
  blaze_ss, treez_ss, webjoint_ss, leaflogix_ss, flowhub_ss, akerna_ss, greenbits_ss, lightspeed_ss,
  meadow_ss, posabit_ss, proteus_ss, growflow_ss
)

# If a room has NA name and ID, and it has no stock, it is a "NO STOCK" room. If it is missing any
# of the ID or name (despite it has or not stock), then assign "UNKNOWN"
population_ss <- mutate(
  population_ss,
  room_id = case_when(
    is.na(room_id) & is.na(room_name) & units_in_stock == 0 ~ "NO STOCK",
    is.na(room_id) ~ "UNKNOWN",
    TRUE ~ room_id
  ),
  room_name = case_when(
    room_id == "NO STOCK" ~ "NO STOCK",
    is.na(room_name) ~ "UNKNOWN",
    TRUE ~ room_name
  )
)

#' Aux fun to get the last non-NA value of a vector. Will return NA if all of them are NA.
last_non_na <- function(x) tail(c(x, x[!is.na(x)]), n = 1)

# Fix: Each SKU must have the same metadata for each org. I.e., keep the last name, brand, etc.
population_ss <- group_by(population_ss, pos, snapshot_utc, org_uuid, sku) |>
  mutate_at(
    c(
      "product_name", "brand_id", "brand", "image", "unit_price", "unit_of_measure",
      "cost_per_unit", "raw_category_name", "raw_product_class"
    ),
    last_non_na
  ) |>
  group_by(
    pos, snapshot_utc, org_uuid, store_uuid, facility, room_id, sku, product_name, brand_id, brand,
    room_name, image, unit_price, unit_of_measure, cost_per_unit, raw_category_name,
    raw_product_class
  ) |>
  summarise(units_in_stock = sum(units_in_stock, na.rm = TRUE), .groups = "drop") |>
  filter(!is.na(sku) & nzchar(sku) & !is.na(units_in_stock))

# Get consolidated categories classification.
pg <- dbc(db_cfg, "consolidated")
population_ss$category3 <- build_category3(population_ss, pg)
default_cats_map <- get_categories_mapping() |>
  select(raw_category_name, default_category_map = product_category_name)
orgs_cats_map <- dbGetQuery(pg, "
  SELECT raw_category_name, product_category_name AS org_category_map, org_uuid
  FROM product_categories
  WHERE org_uuid IS NOT NULL
")
class_map <- dbReadTable(pg, "product_classes") |> select(-run_date_utc)
dbd(pg)

# Clean standardized formats.
population_ss <- mutate_at(population_ss, c("raw_category_name", "raw_product_class"), str_to_lower)
population_ss <- left_join(population_ss, default_cats_map, by = "raw_category_name") |>
  left_join(orgs_cats_map, by = c("org_uuid", "raw_category_name")) |>
  # Prioritize the org-specific category mapping over the default one.
  mutate(product_category_name = coalesce(org_category_map, default_category_map)) |>
  select(-org_category_map, -default_category_map) |>
  left_join(class_map, by = "raw_product_class") |>
  mutate(
    brand_name = str_clean(brand),
    category = str_clean(product_category_name),
    product_name = str_clean(product_name),
    classification = str_clean(product_class)
  )

pop_ss_dt <- as.data.table(population_ss)
pop_ss_dt <- clean_product_categories(pop_ss_dt)
pop_ss_dt <- clean_product_classification(pop_ss_dt)
pop_ss_dt <- clean_product_brands(pop_ss_dt)
population_ss$category2 <- pop_ss_dt$category2
population_ss$classification2 <- pop_ss_dt$classification2
population_ss$brand <- pop_ss_dt$brand_name
population_ss <- mutate(
  population_ss,
  classification2 = if_else(
    category2 == "GLASS_PARAPH" | category2 == "ACCESSORY_MERCH", "None", classification2
  )
)
rm(pop_ss_dt)


# Write -------------------------------------------------------------------
pg <- dbc(db_cfg, "integrated")
org_uuids <- unique(population_ss$org_uuid)
push_res <- lapply(org_uuids, function(current_org_uuid) {
  try(dbWriteTable(
    pg, "rooms_stock_snapshot", filter(population_ss, org_uuid == !!current_org_uuid),
    append = TRUE
  ), silent = TRUE)
})
dbExecute(pg, "ANALYZE rooms_stock_snapshot")
dbd(pg)

# Error handling ----------------------------------------------------------
errored_orgs <- org_uuids[sapply(push_res, function(x) inherits(x, "try-error"))]

# Notify fail in case all of the orgs failed to be pushed.
if (length(errored_orgs) == length(org_uuids)) {
  stop(paste0("All of the orgs (", length(org_uuids), ") failed to be pushed."))
} else if (length(errored_orgs) > 0) {
  warning(paste0(
    "Rooms stock snapshot failed to push for orgs: ", paste(errored_orgs, collapse = ", ")
  ))
}
