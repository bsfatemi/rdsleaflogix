# Consolidated order_lines Data
# build_socalgreenbuddy_flatfile_order_lines.R
#
# (C) 2020 Happy Cabbage Analytics Inc.

#
## ORDER TOTALS -------------------------
#

build_io_flatfile_order_lines <- function(raw_orders, patients, cats_map, org, store) {

  ### PARAMETERS
  source_system <- "indicaonline"


  # DeDup order_lines
  orders_clean <- raw_orders %>%
    group_by(run_date_utc) %>%
    fill(type, order_number, order_source,
      patient, customer_type, created_at, status,
      fulfillment, metrc, metrc_status,
      .direction = "down"
    ) %>%
    fill(subtotal, sales_tax, shipping,
      total, total_discount_amount,
      .direction = "up"
    ) %>%
    mutate(
      line_key = if_else(is.na(qty), "Order Total", "Order Line")
    ) %>%
    ungroup()

  # DeDup order_lines
  order_lines_dedup <- orders_clean %>%
    filter(line_key == "Order Line") %>%
    group_by(order_number, product) %>%
    filter(row_number(created_at) == max(row_number(created_at))) %>%
    ungroup()

  order_lines_dedup <- filter(
    order_lines_dedup, !status %in% c("Cancelled", "Declined", "Rejected", "Returned")
  )

  # Info Needed From Patients
  cust_ids <- patients %>%
    group_by(first_name, last_name) %>%
    filter(row_number(total_completed_orders) == max(row_number(total_completed_orders))) %>%
    ungroup() %>%
    transmute(
      patient = paste(first_name, last_name),
      customer_id = patient_id
    )

  # Clean Up order_lines Info
  order_lines_info <- order_lines_dedup %>%
    mutate(
      org = !!org,
      source_system = !!source_system,
      source_run_date_utc = run_date_utc,
      order_id = order_number,
      product_name = str_to_upper(product),
      product_id = as.character(NA),
      product_uom = uom,
      product_unit_count = as.numeric(qty),
      product_qty = as.numeric(qty),
      raw_category_name = str_to_lower(product_category),
      product_class = case_when(
        str_detect(product_name, "SATIVA") ~ "sativa",
        str_detect(product_name, "INDICA") ~ "indica",
        str_detect(product_name, "CBD") ~ "cbd",
        TRUE ~ "hybrid"
      ),
      order_line_discount = as.numeric(NA),
      order_line_subtotal = as.numeric(NA),
      order_line_tax = as.numeric(NA),
      order_line_total = as.numeric(amount),
      order_line_list_price = as.numeric(price)
    ) %>%
    group_by(order_id) %>%
    mutate(order_line_id = as.character(row_number(product_name))) %>%
    ungroup()

  # Create some brands
  brands <- order_lines_info %>%
    distinct(product_name) %>%
    mutate(
      brand_name = map_chr(product_name, function(x) {
        str_sub(x, 1, coalesce(str_locate(x, " - ")[1], -1)) %>%
          str_replace_all("[[:punct:]]", "") %>%
          str_squish()
      }),
      brand_id = as.character(NA)
    )

  df <- order_lines_info %>%
    left_join(cust_ids, by = "patient") %>%
    filter(!is.na(customer_id)) %>%
    left_join(cats_map, by = "raw_category_name") %>%
    left_join(brands, by = "product_name") %>%
    select(
      org,
      source_system,
      source_run_date_utc,
      customer_id,
      order_id,
      order_line_id,
      product_id,
      product_name,
      brand_id,
      brand_name,
      product_uom,
      product_unit_count,
      product_qty,
      product_category_name,
      product_class,
      order_line_discount,
      order_line_subtotal,
      order_line_tax,
      order_line_total,
      order_line_list_price,
      raw_category_name
    ) %>%
    mutate(
      raw_product_class = NA_character_,
      product_sku = NA_character_
    )

  return(df)
}
