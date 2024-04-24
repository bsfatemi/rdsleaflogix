# libs -------------------------------------------------------------------
box::use(
  dplyr[
    arrange, bind_rows, coalesce, desc, distinct, filter, group_by, left_join, mutate, transmute,
    ungroup
  ],
  fs[path_ext, path_package],
  hcaconfig[get_org_stores],
  httr[add_headers, content, GET],
  jsonlite[fromJSON],
  lubridate[as_datetime],
  purrr[map_chr, map_dfr],
  yaml[read_yaml]
)

# utils ------------------------------------------------------------------
# grabs all credentials for akerna clients in raw form
..fp <- function(f) {
  read_yaml(path_package("hcaakerna", "extdata", path_ext(f), f))
}

keyIndex <- function(org_index) {
  # Get yaml file credentials.
  map_dfr(..fp("src-akerna.yml")[[1]]$creds$clients, function(org_creds) { # For each org_uuid.
    org_uuid <- names(org_creds)
    org_creds <- org_creds[[1]]
    # Unnamed stores case.
    if (length(setdiff(c("apikey", "org_id", "user_id", "facility_id"), names(org_creds))) == 0) {
      org_creds <- data.frame(c(org_creds), facility = "main", org_uuid = org_uuid)
    } else {
      # Save org-level creds ("all") for later, delete from facilities
      org_all <- org_creds[["all"]]
      org_creds[["all"]] <- NULL
      # Sort stores alphabetically by name.
      facilities <- org_creds[order(names(org_creds))]
      org_creds <- cbind(
        bind_rows(lapply(names(facilities), function(facility) {
          data.frame(
            org_uuid = org_uuid, facility = facility, facility_id = facilities[[facility]]
          )
        })),
        org_all
      )
    }
    org_creds$short_name <- filter(org_index, org_uuid == !!org_uuid)$short_name
    left_join(
      # Fix some unmatched facility names.
      mutate(org_creds, facility = gsub("^naturesmed_salida$", "natures_med_salida", facility)),
      distinct(get_org_stores(org_uuid), facility, store_uuid),
      by = "facility"
    )
  })
}

# endpoint getter --------------------------------------------------------
get_akerna <- function(org_creds, endpoint, ...) {
  res <- data.frame()
  last_page <- Inf
  i <- 1
  query <- list(...)
  while (i <= last_page) {
    message(
      "Fetching ", endpoint, " for ", org_creds$short_name, " - ", org_creds$facility, ": page ", i
    )
    query$page <- i
    resp <- GET(
      url = paste0("https://partner-gateway.mjplatform.com/v1/", endpoint),
      query = query,
      add_headers(
        "x-mjf-api-key"         = org_creds$apikey,
        "x-mjf-organization-id" = org_creds$org_id,
        "x-mjf-facility-id"     = as.character(org_creds$facility_id),
        "x-mjf-user-id"         = org_creds$user_id,
        "Accept"                = "application/json",
        "Content-Type"          = "application/json",
        "Cache-Control"         = "no-cache"
      )
    )
    resp_res <- fromJSON(content(resp, as = "text", encoding = "UTF-8"))
    if ("data" %in% names(resp_res)) {
      res <- bind_rows(res, resp_res$data)
    } else {
      res <- bind_rows(res, data.frame(t(unlist(resp_res))))
    }
    if ("last_page" %in% names(resp_res)) {
      last_page <- resp_res$last_page
    } else {
      last_page <- i
    }
    i <- i + 1
  }
  res
}

# all --------------------------------------------------------------------
inventory_snapshot <- function(org_creds) {
  items <- get_akerna(org_creds, "items")
  catalog <- get_akerna(org_creds, "catalog", include_pricing_details = 1)
  # De-dupe catalog by SKU. First keep rows with id in items, then keep the last updated by SKU.
  catalog <- filter(catalog, id %in% items$item_master_id) |>
    arrange(desc(as_datetime(updated_at))) |>
    distinct(sku = item_number, .keep_all = TRUE)
  # Fix the pricing: we prioritize medical pricing. Current customers are only medical, and Akerna,
  # in these cases returns $0 for recreational prices.
  catalog <- mutate(
    catalog,
    price_recreational = as.numeric(detailed_pricing$recreational$catalog_price),
    price_medical = as.numeric(detailed_pricing$medical$catalog_price),
    # Use the medical price if it is >= $5, if not the max of both prices.
    unit_price = ifelse(price_medical >= 5, price_medical, pmax(price_medical, price_recreational))
  )
  # De-dupe items by item_master_id. Keep the last updated item, and sum the quantities per room.
  items <- group_by(items, item_master_id, room_id = inventory_location_id) |>
    mutate(units_in_stock = sum(qty)) |>
    arrange(desc(as_datetime(updated_at))) |>
    distinct(item_master_id, room_id, .keep_all = TRUE) |>
    ungroup()
  catalog$image <- map_chr(
    catalog$primary_image_urls, ~ ifelse(is.null(.x), NA_character_, .x$url[[1]])
  )
  left_join(items, catalog, by = c(item_master_id = "id")) |>
    transmute(
      org_uuid = org_creds$org_uuid,
      store_uuid = org_creds$store_uuid,
      facility = org_creds$facility,
      room_id,
      product_id = id,
      sku,
      product_name = item_name,
      brand_id,
      brand = coalesce(brand_name, product_name),
      room_name = location_name,
      units_in_stock,
      image,
      unit_price,
      unit_of_measure = uom,
      # cost_per_unit,
      raw_category_name = category_name,
      raw_product_class = strain_name
    ) |>
    # Filter out NA skus resulting from the join
    filter(!is.na(sku), nchar(sku) > 0)
}
