box::use(
  logger[log_info]
)

build_population2 <- function(POP) {
  POP <- data.table::copy(POP)

  # RENAME - columns ---------------------------------------------------------
  old <- c(
    "order_line_total",
    "order_line_subtotal",
    "order_line_discount",
    "order_line_tax",
    "order_line_list_price",
    "order_total",
    "order_subtotal",
    "order_discount",
    "order_id",
    "order_line_id",
    "product_id",
    "raw_category_name"
  )
  new <- c(
    "item_total",
    "item_subtotal",
    "item_discount",
    "item_tax",
    "item_list_price",
    "order_tot",
    "order_subtot",
    "order_disc",
    "pos_order_id",
    "pos_order_line_id",
    "pos_product_id",
    "pos_category"
  )
  data.table::setnames(POP, old, new)

  old <- c("product_category_name", "product_class")
  new <- c("category", "classification")
  data.table::setnames(POP, old, new)
  log_info("Renaming population1 columns to population2 columns")

  # CLEAN - string columns --------------------------------------------------
  tmp <- c(
    "phone",
    "email",
    "first_name",
    "last_name",
    "product_name",
    "brand_name",
    "category",
    "classification",
    "order_facility",
    "order_type"
  )

  for (col in tmp) {
    data.table::set(
      x = POP,
      i = NULL,
      j = col,
      value = pipelinetools::str_clean(POP[, get(col)])
    )
  }

  log_info("Create `order_id` specific to `org_uuid`, `store_id` and `pos_order_id`.")
  POP[, order_id := uuid::UUIDgenerate(), .(org_uuid, store_id, pos_order_id)]

  log_info("Assigning NA_character_ to empty strings")
  POP[category == "",       category := NA_character_]
  POP[brand_name == "",     brand_name := NA_character_]
  POP[product_name == "",   product_name := NA_character_]
  POP[classification == "", classification := NA_character_]
  POP[, product_name := stringr::str_to_upper(product_name)]

  # If category, brand_name and product_name are NA, then assign them "HOUSE".
  POP[
    is.na(category) & is.na(brand_name) & is.na(product_name),
    c("brand_name", "product_name", "category") := "HOUSE"
  ]
  # If we have FLOWERs with brand_name and no product_name, then assign the brand as product_name.
  POP[(!is.na(brand_name)) & is.na(product_name) & category == "FLOWER", product_name := brand_name]

  log_info("Cleaning category via pipelinetools functions")
  POP <- pipelinetools::clean_product_categories(POP)
  log_info("Cleaning classification via pipelinetools functions")
  POP <- pipelinetools::clean_product_classification(POP)
  log_info("Cleaning brands via pipelinetools functions")
  POP <- pipelinetools::clean_product_brands(POP)

  # Remove old columns.
  log_info("Remove category1 and classification1 columns, clean gender column")
  POP[, category := NULL]
  POP[, classification := NULL]

  POP[, gender := stringr::str_extract(gender, "^[MF]")]
  POP[is.na(gender), gender := "U"]

  # SET - Amount columns -------------------------------------------------------
  log_info("Set tax, order_discount, and item_discount to 0 when NA")
  POP[is.na(order_disc), order_disc := 0]
  POP[is.na(item_tax), item_tax := 0]
  POP[is.na(item_discount), item_discount := 0]

  log_info("Calculating subtotal, item_total, item_list_price for different NA values")
  POP[
    is.na(item_subtotal) & !is.na(item_list_price) & !is.na(item_discount),
    "item_subtotal" := item_list_price + item_discount
  ]
  POP[
    is.na(item_total) & !is.na(item_list_price) & !is.na(item_discount) & !is.na(item_tax),
    "item_total" := item_subtotal + item_discount + item_tax
  ]
  POP[
    is.na(item_list_price) & !is.na(item_subtotal),
    "item_list_price" := item_subtotal
  ]
  POP[stringr::str_detect(first_name, "^[0-9]+$"), first_name := NA]

  log_info("Subset pop2 to include only columns needed")
  POP2 <- POP[, .(
    org_uuid,
    order_id,
    order_facility,
    facility,
    store_id,
    order_type,
    order_time_utc,
    customer_id,
    first_name,
    last_name,
    gender,
    birthday,
    age,
    phone,
    product_name,
    product_sku,
    brand_name,
    category2,
    category3,
    classification2,
    product_qty,
    item_total,
    item_subtotal,
    item_list_price,
    item_discount,
    order_tot,
    order_subtot,
    order_disc,
    pos_order_id,
    pos_order_line_id,
    pos_product_id,
    pos_category
  )]

  log_info("Add timestamps to pop2")
  POP2[, order_time_utc := lubridate::as_datetime(order_time_utc, tz = "UTC")]
  POP2[, wk_start_date := lubridate::floor_date(order_time_utc, "week", 1)]

  log_info("Set key columns for pop2")
  keyCols <- c("org_uuid", "order_facility", "order_time_utc", "order_id")
  data.table::setkeyv(POP2, keyCols)
  return(POP2)
}
