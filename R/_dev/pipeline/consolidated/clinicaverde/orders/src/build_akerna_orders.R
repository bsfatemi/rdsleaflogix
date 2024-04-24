# Consolidated Orders Data
# build_akerna_orders.R
#
# (C) 2021 Happy Cabbage Analytics Inc.

box::use(
  dplyr[
    coalesce, distinct, filter, group_by, if_else, inner_join, left_join, mutate, row_number,
    ungroup
  ],
  lubridate[as_datetime, now, with_tz, ymd_hms],
  pipelinetools[process_zipcode],
  stringr[str_squish, str_to_lower, str_to_upper]
)

build_akerna_orders <- function(orders, addresses, org, order_facility, facility, tz, store_id) {
  ## DEDUP ORDERS
  orders <- orders |>
    group_by(order_id = as.character(id)) |>
    filter(order_status == "completed")

  ## PARSE DELIVERY ADDRESSES
  delivery_orders <- orders |>
    filter(fulfillment_method == "delivery") |>
    distinct(order_id = as.character(id), customer_id = as.character(consumer_id))

  addresses <- addresses |>
    group_by(customer_id = as.character(consumer_id)) |>
    filter(row_number(updated_at) == max(row_number(updated_at))) |>
    ungroup() |>
    inner_join(delivery_orders, by = "customer_id") |>
    distinct(
      order_id,
      delivery_order_city = city,
      delivery_order_state = province_code,
      delivery_order_zipcode = process_zipcode(postal_code),
      delivery_order_street1 = str_to_upper(street_address_1),
      delivery_order_street2 = str_to_upper(street_address_2),
      delivery_order_address = if_else(
        is.na(delivery_order_zipcode), NA_character_,
        str_squish(paste(
          coalesce(delivery_order_street1, ""),
          coalesce(delivery_order_street2, ""),
          coalesce(delivery_order_city, ""),
          coalesce(delivery_order_state, ""),
          delivery_order_zipcode
        ))
      )
    )

  # Standardize
  output <- orders |>
    mutate(
      order_id = as.character(id),
      source_run_date_utc = run_date_utc,
      customer_id = as.character(consumer_id),
      order_type = if_else(fulfillment_method == "delivery", "Delivery", "Retail"),
      sold_by = str_to_upper(completed_by_user_name),
      sold_by_id = as.character(completed_by_user_id),
      order_time_local = format(with_tz(
        ymd_hms(created_at),
        tz
      ), "%Y-%m-%d %H:%M:%S"),
      order_time_utc = format(ymd_hms(created_at), "%Y-%m-%d %H:%M:%S"),
      delivery_order_received_local = if_else(
        order_type == "Delivery", order_time_local, format(as_datetime(NA), "%Y-%m-%d %H:%M:%S")
      ),
      delivery_order_started_local = NA_character_,
      delivery_order_complete_local = format(
        if_else(order_type == "Delivery", with_tz(ymd_hms(completed_date), tz), as_datetime(NA)),
        "%Y-%m-%d %H:%M:%S"
      ),
      order_tax = as.numeric(tax_total),
      order_total = as.numeric(order_total),
      order_subtotal = order_total - order_tax,
      order_discount = as.numeric(coupon_total) * -1,
      org = org,
      source_system = "akerna",
      run_date_utc = now(tzone = "UTC"),
      facility = facility,
      order_facility = order_facility,
      store_id = store_id,
      geocode_type = "regular"
    ) |>
    left_join(addresses, by = "order_id")

  return(output)
}
