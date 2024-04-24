# Seed Talent API PL
# build_budtenders.R
# (C) Happy Cababge Analytics, Inc. 2022

box::use(
  dplyr[case_when, group_by, left_join, mutate, n_distinct, rename, summarize],
  lubridate[date, days, today]
)

build_pop_info <- function(pop, preorders = NULL, t1_days = 30) {
  ## TODO: EXTRA DATA ON ORDER SOURCE AND PREORDER AMOUNTS NEEDS TO BE MOVED INTO
  #        COLUMNS IN POPULATION
  # VALUES
  popOrderInfo <- pop |>
    group_by(
      order_id,
      order_source,
      order_type,
      store = facility,
      store_uuid = store_id,
      order_time_local,
      order_time_utc,
      sold_by = toupper(sold_by),
      sold_by_id,
      customer_id,
      user_type,
      phone,
      order_total,
      order_discount,
      order_subtotal,
      order_tax
    ) |>
    summarize(
      order_lines = n_distinct(order_line_id),
      products = n_distinct(product_id),
      brands = n_distinct(brand_name),
      categories = n_distinct(raw_category_name),
      .groups = "drop"
    ) |>
    mutate(
      order_dt = date(order_time_local),
      in_eval_period = order_dt >= today() - days(t1_days),
      # If `was_pickup` means it was a preorder.
      was_pickup = case_when(
        order_type == "Delivery" ~ FALSE,
        tolower(order_source) %in% c(
          "walk in", "in_store", "in-store", "ios-instore", "walk_in", "walk-in", "unknown", ""
        ) ~ FALSE,
        is.na(order_source) | is.null(order_source) ~ FALSE,
        TRUE ~ TRUE
      )
    )

  if (!is.null(preorders)) {
    popOrderInfo <- popOrderInfo |>
      left_join(rename(preorders, store = facility), by = c("store", "phone", "order_dt")) |>
      mutate(
        is_upsell = round(order_subtotal - order_discount) > round(item_lvl_list_priice),
        is_downsell = round(order_subtotal - order_discount) < round(item_lvl_list_priice),
        is_unit_upsell = order_lines > item_qty,
        is_unit_downsell = order_lines < item_qty,
        preorder_change_dollars = (order_subtotal - order_discount) - item_lvl_list_priice,
        preorder_change_units = order_lines - item_qty,
        channel = case_when(
          was_pickup == FALSE & order_type == "Retail" ~ "WALK-IN",
          was_pickup == TRUE & !is.na(amount) & order_type == "Retail" ~ "PREORDER",
          was_pickup == TRUE & is.na(amount) & order_type == "Retail" ~ "PREORDER - NO ECOMM",
          order_type == "Delivery" ~ "DELIVERY",
          TRUE ~ "OTHER"
        )
      )
  } else {
    popOrderInfo <- popOrderInfo |>
      mutate(
        is_upsell = NA_real_,
        preorder_change_dollars = NA_real_
      ) |>
      mutate(
        channel = case_when(
          was_pickup == FALSE & order_type == "Retail" ~ "WALK-IN",
          was_pickup == TRUE & order_type == "Retail" ~ "PREORDER - NO ECOMM",
          order_type == "Delivery" ~ "DELIVERY",
          TRUE ~ "OTHER"
        )
      )
  }

  return(popOrderInfo)
}
