# Seed Talent API PL
# build_jane.R
# (C) Happy Cababge Analytics, Inc. 2022

box::use(
  dplyr[filter, group_by, mutate, mutate_at, n_distinct, summarize, vars],
  lubridate[date, with_tz],
  pipelinetools[process_phone]
)

build_jane_customer_preorders <- function(jane, tz = "America/New_York") {
  pick_up_info <- jane |>
    filter(reservation_mode %in% c("pickup", "kiosk")) |>
    mutate_at(
      vars(items, item_lvl_list_price, item_lvl_subtotal, item_qty),
      as.numeric
    ) |>
    mutate(
      phone = process_phone(customer_phone_number),
      pickup_id = as.character(id),
      user_id = as.character(user_id),
      update_to_res = as.numeric(difftime(
        updated_at_time, reservation_start_window, units = "mins"
      )),
      res_to_checkout = as.numeric(difftime(
        reservation_start_window, checked_out_time, units = "mins"
      )),
      created_to_checkout = as.numeric(difftime(checked_out_time, created_at, units = "mins"))
    )
  customer_jane <- pick_up_info |>
    filter(!is.na(phone)) |>
    group_by(
      facility, phone,
      order_dt = date(with_tz(reservation_start_window, tz))
    ) |>
    summarize(
      amount = sum(amount),
      orders = n_distinct(pickup_id),
      items = sum(items),
      item_lvl_list_priice = sum(item_lvl_list_price),
      item_lvl_subtotal = sum(item_lvl_subtotal),
      item_qty = sum(item_qty),
      .groups = "drop"
    )

  return(customer_jane)
}
