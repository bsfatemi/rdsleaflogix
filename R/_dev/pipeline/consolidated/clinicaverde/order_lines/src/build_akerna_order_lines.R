# Consolidated Order Lines Data
# build_akerna_order_lines.R
#
# (C) 2021 Happy Cabbage Analytics Inc.
#
## ORDER LINES -------------------------

box::use(
  dplyr[case_when, coalesce, distinct, filter, left_join, mutate, transmute],
  lubridate[now],
  stringr[str_detect, str_to_lower, str_to_upper],
)

build_akerna_order_lines <- function(order_details, item_master, catalog, class_map, cats_map,
                                     org) {
  order_details <- order_details |>
    mutate(order_line_id = as.character(products_id)) |>
    filter(order_status == "completed")
  item_master <- item_master |>
    mutate(id = as.character(id))
  catalog <- catalog |>
    mutate(product_id = as.character(id))


  ### BUILD DATA
  class_info <- distinct(catalog, product_id, raw_product_class = str_to_lower(dominance)) |>
    left_join(class_map, by = "raw_product_class")
  product_info <- item_master |>
    transmute(
      subcategory_id,
      product_id = as.character(id),
      product_name = name,
      brand_name = coalesce(brand_name, product_name),
      brand_id = as.character(brand_id),
      raw_category_name = case_when(
        str_detect(str_to_lower(subcategory_name), "pre-roll") ~ "pre-roll",
        category_name == "Concentrate" ~ "extract",
        TRUE ~ str_to_lower(category_name)
      ),
      product_uom = default_uom,
      product_unit_count = 1,
      product_sku = item_number
    ) |>
    left_join(cats_map, by = "raw_category_name")

  filter(order_details, order_status == "completed") |>
    mutate(
      source_run_date_utc = run_date_utc,
      customer_id = as.character(consumer_id),
      order_id = as.character(id),
      product_id = as.character(products_item_master_id),
      product_qty = as.numeric(products_quantity),
      order_line_discount = as.numeric(products_discount_total) * -1,
      order_line_list_price = as.numeric(products_unit_price) * product_qty,
      order_line_subtotal = order_line_list_price + order_line_discount,
      order_line_tax = ifelse(
        order_total - tax_total != 0, tax_total / (order_total - tax_total) * order_line_subtotal, 0
      ),
      order_line_total = order_line_tax + order_line_subtotal,
      org = org,
      source_system = "akerna",
      run_date_utc = now(tzone = "UTC")
    ) |>
    left_join(product_info, by = "product_id") |>
    left_join(class_info, by = "product_id") |>
    distinct()
}
