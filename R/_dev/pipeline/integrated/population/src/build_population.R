box::use(
  DBI[dbExistsTable],
  dbplyr[...], # Called to be used as backend for dplyr.
  dplyr[
    any_of, coalesce, collect, distinct, filter, inner_join, left_join, mutate, mutate_at,
    rename_at, select, tbl, tibble, transmute, vars
  ],
  logger[log_info],
  lubridate[as_datetime, with_tz],
  pipelinetools[db_read_table_unique],
  purrr[map_lgl, map2_chr]
)

box::use(
  inst / pipeline / integrated / population / src / build_category3[build_category3]
)

build_population <- function(pg, org) {
  # Columns we want from customers
  cust_cols <- c(
    "org", "customer_id", ## INDEX and JOIN COLUMNS
    "created_at_utc", "last_updated_utc",
    "phone", "first_name", "last_name", "gender",
    "birthday", "age", "email", "user_type", "geocode_type", "long_address",
    "address_street1", "address_street2", "city", "state", "zipcode", "latitude",
    "longitude", "pos_is_subscribed", "loyalty_pts", "tags"
  )

  # Columns to rename with the "customer_" abbreviation
  cust_rm_cols <- c(
    "created_at_utc", "last_updated_utc", "geocode_type", "long_address",
    "address_street1", "address_street2", "city", "state", "zipcode",
    "latitude", "longitude"
  )

  # Columns we want from orders
  order_cols <- c(
    "org", "source_system", "customer_id", "order_id", ## INDEX and JOIN COLUMNS
    "pos_short_order_id", "order_facility", "order_type", "sold_by_id",
    "order_time_local", "order_time_utc", "delivery_order_received_local",
    "delivery_order_started_local", "delivery_order_complete_local",
    "order_subtotal", "order_tax", "order_total", "order_discount",
    "delivery_order_address", "delivery_order_street1", "delivery_order_street2",
    "delivery_order_city", "delivery_order_zipcode", "delivery_order_state",
    "delivery_lon", "delivery_lat", "facility", "store_id", "order_source"
  )

  # Columns we want from orders
  ol_cols <- c(
    "order_id", "source_system", "org", ## INDEX and JOIN COLUMNS
    "order_line_id", "product_id", "product_name", "brand_id", "brand_name",
    "product_uom", "product_unit_count", "product_qty",
    "product_category_name", "product_class", "order_line_total",
    "order_line_discount", "order_line_tax", "order_line_subtotal",
    "order_line_list_price", "raw_product_class", "raw_category_name", "product_sku"
  )

  log_info("Checking if Config structure has index table")
  if (is.null(org$order_lines)) {
    print(paste(org$short_name, "doesn't have order_lines, skipping"))
    return(tibble())
  }

  log_info("Checking consolidated tables are there too in the database")
  tbls_exist <- map_lgl(c(org$order_lines, org$customers, org$orders), ~ dbExistsTable(pg, .x))

  if (!all(tbls_exist)) {
    print(paste(org$short_name, "one or more tables do not exist, skipping"))
    return(tibble())
  }

  log_info("Reading in order_lines table with minor cleaning")

  ol <- tbl(pg, org$order_lines) |>
    select(any_of(ol_cols)) |>
    distinct() |>
    filter(!is.na(org), !is.na(order_line_id), !is.na(order_id), !is.na(source_system)) |>
    collect()

  log_info("Reading in orders table with selected columns")

  o <- tbl(pg, org$orders) |>
    select(any_of(order_cols)) |>
    distinct() |>
    collect()

  log_info("Reading in customers table with selected columns")

  c <- tbl(pg, org$customers) |>
    select(any_of(cust_cols)) |>
    distinct() |>
    collect()

  log_info("Fix employee names duplication.")
  employees <- db_read_table_unique(
    pg, org$orders, "sold_by_id", "order_time_utc", c("sold_by_id", "sold_by")
  )
  o <- left_join(o, employees, by = "sold_by_id")

  # With `inner_join` we are discarding incomplete data that needs to be backfilled. However,
  # it is better to show less data, than the app crashing for NA values.
  # We allow order_lines with NA customers, but not with NA orders.

  log_info("Joining order_lines, orders, and customers")
  org_population <- ol |>
    inner_join(o, by = c("source_system", "org", "order_id")) |>
    left_join(c, by = c("customer_id", "org"))

  log_info("Add onfleet delivery data and rename columns selected from the customers table.")

  org_population <- rename_at(org_population, vars(any_of(cust_rm_cols)), ~ paste0("customer_", .))
  org_population <- add_deliveries_data(org_population, pg) |>
    select(-any_of(c("pos_short_order_id"))) |>
    mutate(
      gender = coalesce(gender, "U"),
      age = coalesce(age, 0),
      brand_name = coalesce(brand_name, "NO_VALUE")
    )

  log_info("Start building category3 column.")

  org_population$category3 <- build_category3(org_population, pg)

  log_info("Finish building category3 column.")

  # Transformations to avoid frequent issues we are having.
  # IDs must be character.
  org_population <- mutate_at(org_population, vars(ends_with("_id")), as.character)

  # Add the `org_uuid`.
  org_population$org_uuid <- org$org_uuid

  return(org_population)
}

add_deliveries_data <- function(org_population, pg) {
  # If there is no data, then there's nothing to add.
  if (nrow(org_population) == 0) {
    return(org_population)
  }
  log_info("Begin reading in deliveries table by org.")
  # If there is a deliveries table for the org, then add the data.
  deliveries_table <- paste0(org_population$org[[1]], "_deliveries")
  if (dbExistsTable(pg, deliveries_table)) {
    deliveries <- db_read_table_unique(
      pg, deliveries_table, "pos_short_order_id", "created_at_utc",
      columns = c("pos_short_order_id", "delivery_start_utc", "delivery_complete_utc", "timezone")
    ) |>
      # Transform time data to the correct timezone.
      transmute(
        pos_short_order_id,
        of_delivery_order_started_local = map2_chr(timezone, delivery_start_utc, ~ format(
          with_tz(.y, .x), "%Y-%m-%d %H:%M:%S"
        )),
        of_delivery_order_complete_local = map2_chr(timezone, delivery_complete_utc, ~ format(
          with_tz(.y, .x), "%Y-%m-%d %H:%M:%S"
        ))
      )
    log_info("Finished reading deliveries table.")
    # Onfleet can return a short order ID or a regular order ID with no differentiation,
    # joining on short order then coalescing so we have order ID for all.
    deliveries <- left_join(
      deliveries, distinct(org_population, order_id, pos_short_order_id),
      by = "pos_short_order_id"
    ) |>
      mutate(order_id = coalesce(order_id, pos_short_order_id)) |>
      select(-pos_short_order_id) |>
      distinct(order_id, .keep_all = TRUE)

    log_info("Joining deliveries table to population table.")

    # Add deliveries data. Prioritizing deliveries table.
    org_population <- left_join(org_population, deliveries, by = "order_id") |>
      mutate(
        delivery_order_started_local = coalesce(
          of_delivery_order_started_local, delivery_order_started_local
        ),
        delivery_order_complete_local = coalesce(
          of_delivery_order_complete_local, delivery_order_complete_local
        )
      ) |>
      select(-of_delivery_order_started_local, -of_delivery_order_complete_local)
  }
  log_info("Calculating `delivery_drive_minutes`.")
  # Calculate `delivery_drive_minutes`.
  if (all(
    c("delivery_order_started_local", "delivery_order_complete_local") %in% colnames(org_population)
  )) {
    org_population <- mutate(org_population, delivery_drive_minutes = as.numeric(difftime(
      as_datetime(delivery_order_complete_local),
      as_datetime(delivery_order_started_local),
      units = "mins"
    )))
  }
  log_info("Finished adding deliveries data to population table.")
  org_population
}
