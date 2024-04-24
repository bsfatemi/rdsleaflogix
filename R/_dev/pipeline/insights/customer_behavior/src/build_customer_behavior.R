# Marketing Studio App Data
# build_customer_behavior
#
# (C) 2020 Happy Cabbage Analytics Inc.


## Order Information
build_order_behavior <- function(population) {
  # Order frequency, latency, and size
  customer_order_behavior <- lazy_dt(population) |>
    distinct(
      customer_id, org, order_time_local, order_id,
      order_time_utc, order_time_local,
      order_discount, order_total, order_type, order_facility, order_source
    ) |>
    mutate(
      order_time_utc      = ymd_hms(order_time_utc),
      order_date_numeric  = as.numeric(date(order_time_local)),
      order_hour_numeric  = as.numeric(hour(ymd_hms(order_time_local))),
    ) |>
    group_by(customer_id, org) |>
    arrange(order_time_local) |>
    mutate(
      time_between_orders = fcoalesce(order_date_numeric - shift(order_date_numeric, 1), 0),
      number_orders = n_distinct(order_id, na.rm = TRUE),
      total_purchases_dollars = sum(order_total, na.rm = TRUE),
      total_discounts_dollars = sum(order_discount, na.rm = TRUE)
    ) |>
    summarize(
      num_orders = number_orders,
      first_order_date = min(order_time_utc, na.rm = TRUE),
      last_order_date = max(order_time_utc, na.rm = TRUE),
      total_purchases_usd = total_purchases_dollars,
      total_discounts_usd = total_discounts_dollars,
      avg_order_size = mean(order_total, na.rm = TRUE),
      tot_discount_pct = coalesce(
        total_discounts_dollars / (total_purchases_dollars + total_discounts_dollars), 0
      ),
      pct_delivery = n_distinct(
        if_else(order_type == "Delivery", order_id, as.character(NA)),
        na.rm = TRUE
      ) / number_orders,
      avg_time_between_orders = mean(time_between_orders, na.rm = TRUE),
      avg_order_time = mean(order_hour_numeric, na.rm = TRUE),
      last_order_facility = last(order_facility, order_by = order_time_utc),
      last_order_source = last(order_source, order_by = order_time_utc),
      .groups = "drop"
    ) |>
    distinct() |>
    collect()
  return(customer_order_behavior)
}

## Brands
build_product_behavior <- function(population) {
  customer_products <- lazy_dt(population) |>
    distinct(
      customer_id, org, brand
    ) |>
    group_by(customer_id, org) |>
    summarize(
      all_order_descr = toString(unique(brand)),
      unique_brands = n_distinct(brand, na.rm = TRUE),
      .groups = "drop"
    ) |>
    collect()
  return(customer_products)
}


### PREFERENCES
build_customer_prefs <- function(population, col) {
  pcts <- lazy_dt(population) |>
    group_by_at(vars(c("customer_id", "org", {{ col }}))) |>
    summarize("num_products_ordered" = n(), .groups = "drop") |>
    group_by(customer_id, org) |>
    mutate(
      pct = num_products_ordered / sum(num_products_ordered, na.rm = TRUE)
    ) |>
    select(customer_id, org, pct, {{ col }}) |>
    ungroup() |>
    distinct() |>
    collect()

  output <- pcts |>
    filter_at(vars({{ col }}), ~ coalesce(., "NA") != "NA") |>
    pivot_wider(
      id_cols = c(customer_id, org),
      names_from = {{ col }},
      names_prefix = "pct_",
      values_from = pct,
      values_fill = list(pct = 0)
    ) |>
    distinct()

  return(output)
}
