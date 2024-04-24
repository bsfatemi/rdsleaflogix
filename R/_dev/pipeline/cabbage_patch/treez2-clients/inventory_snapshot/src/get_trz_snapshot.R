box::use(
  data.table[rbindlist],
  dplyr[
    bind_cols, bind_rows, distinct, filter, group_by, left_join, mutate_at, select, transmute
  ],
  fs[path_package, path_ext],
  hcaconfig[get_org_stores],
  hcatreez[get_api_keys],
  jsonlite[fromJSON],
  purrr[map_dfr],
  stringr[str_remove],
  yaml[read_yaml]
)

# utils -------------------------------------------------------------------
# grabs all credentials for treez clients in raw form
..fp <- function(f) {
  read_yaml(path_package("hcatreez", "extdata", path_ext(f), f))
}

# grabs all credentials for treez clients and organizes them into a wide-format table indexed by
# facility
# end table contains the columns: org_uuid, facility, apikey, short_name, disp_name
keyIndex <- function(org_index) {
  # Get yaml file credentials.
  cfg <- ..fp("src-treez.yml")
  ORGS <- rbindlist(
    lapply(cfg[[1]]$creds$clients, function(org_creds) { # For each org_uuid.
      org_uuid <- names(org_creds)
      org_creds <- org_creds[[1]]
      # Unnamed stores case.
      if (length(setdiff(c("apikey", "disp_name"), names(org_creds))) == 0) {
        org_creds <- list(main = org_creds)
      }
      # Sort stores alphabetically by name.
      org_creds <- org_creds[order(names(org_creds))]
      org_creds <- bind_rows(lapply(names(org_creds), function(facility) {
        data.frame(
          org_uuid = org_uuid, facility = facility, apikey = org_creds[[facility]]$apikey,
          disp_name = org_creds[[facility]]$disp_name
        )
      }))
      org_creds$short_name <- filter(org_index, org_uuid == !!org_uuid)$short_name
      org_creds
    }),
    use.names = TRUE
  ) |>
    # Add `store_uuid` by joining `disp_name = facility`.
    left_join(
      select(get_org_stores(), org_uuid, disp_name = facility, store_uuid),
      by = c("org_uuid", "disp_name")
    )
  ORGS[, "facility" := str_remove(facility, ".*_")]
  # Get database file credentials.
  db_creds <- get_api_keys() |>
    transmute(
      org_uuid, store_uuid,
      facility = store_short_name, apikey, disp_name = dispensary_name,
      short_name = org_short_name
    )
  bind_rows(ORGS, db_creds)
}

grab_product_sku <- function(product_info) {
  if (!"product_barcodes" %in% colnames(product_info)) {
    product_info$sku <- NA_character_
  } else {
    # Keep rows with valid `product_barcodes` data.
    skus <- filter(product_info, !is.na(product_barcodes), product_barcodes != "{}") |>
      select(org_uuid, facility, product_id, product_barcodes)
    # Parse JSONs in a high-performance way.
    parsed_skus <- fromJSON(paste0("[", paste0(skus$product_barcodes, collapse = ","), "]"))
    # To each parsed JSON, add the product's metadata.
    skus <- map_dfr(seq_along(parsed_skus), function(i) bind_cols(skus[i, ], parsed_skus[[i]])) |>
      mutate_at(c("sku", "type"), unlist)
    # For each `org_uuid, facility, product_id`, keep the first appearing SKU, prioritizing by
    # "GENERATED" ones.
    skus <- bind_rows(filter(skus, type == "GENERATED"), skus) |>
      distinct(org_uuid, facility, product_id, .keep_all = TRUE) |>
      select(org_uuid, facility, product_id, sku)
    # Add SKUs info.
    product_info <- left_join(product_info, skus, by = c("org_uuid", "facility", "product_id"))
  }
  product_info
}
