build_customer_vip_points <- function(population) {
  setDT(population)

  summDT <- population[
    !(is.na(phone) & is.na(customer_id)) # Drop if customer does not have ID and phone.
  ][!is.na(item_subtotal) & !is.na(product_qty) & !is.na(order_time_utc)][order(order_time_utc)][
    , .(
      # Get the latest personal data.
      first_name = rev(unique(first_name))[1],
      last_name = rev(unique(last_name))[1],
      birthday = rev(unique(birthday))[1],
      days_since_last_order = floor(
        as.numeric(difftime(now(), max(order_time_utc, na.rm = TRUE), "UTC", "days"))
      ),
      avg_days_between_orders = mean(
        ceiling(as.numeric(diff(unique(order_time_utc)), units = "hours") / 24), na.rm = TRUE
      ),
      avg_order_total = mean(order_tot[!duplicated(order_id)], na.rm = TRUE),
      # This is ordered by `order_time_utc`, so let's get the last price.
      avg_item_price = mean(
        item_subtotal[!duplicated(pos_product_id, fromLast = TRUE)], na.rm = TRUE
      ),
      avg_discount_to_date <- mean(abs(order_disc[!duplicated(order_id)]) / (
        order_tot[!duplicated(order_id)] + abs(order_disc[!duplicated(order_id)])
      ), na.rm = TRUE),
      avg_order_items = round(mean(by(product_qty, order_id, sum, na.rm = TRUE), na.rm = TRUE)),
      total_items = sum(product_qty, na.rm = TRUE),
      total_orders = uniqueN(order_id),
      total_dollars = sum(order_tot[!duplicated(order_id)], na.rm = TRUE)
    ),
    keyby = .(org_uuid, customer_id, phone)
  ]

  # Calculate VIP levels.
  summDT[, `:=`(
    items_low    = quantile(total_items, .50, na.rm = TRUE),
    items_med    = quantile(total_items, .85, na.rm = TRUE),
    items_high   = quantile(total_items, .98, na.rm = TRUE),
    orders_low   = quantile(total_orders, .45, na.rm = TRUE),
    orders_med   = quantile(total_orders, .70, na.rm = TRUE),
    orders_high  = quantile(total_orders, .85, na.rm = TRUE),
    dollars_low  = quantile(total_dollars, .45, na.rm = TRUE),
    dollars_med  = quantile(total_dollars, .70, na.rm = TRUE),
    dollars_high = quantile(total_dollars, .95, na.rm = TRUE)
  ), .(org_uuid)]

  summDT[total_items <= items_low, vip_items := 0]
  summDT[total_items > items_low & total_items <= items_med, vip_items := 1]
  summDT[total_items > items_med & total_items <= items_high, vip_items := 2]
  summDT[total_items > items_high, vip_items := 3]

  summDT[total_orders <= orders_low, vip_orders := 0]
  summDT[total_orders > orders_low & total_orders <= orders_med, vip_orders := 1]
  summDT[total_orders > orders_med & total_orders <= orders_high, vip_orders := 2]
  summDT[total_orders > orders_high, vip_orders := 3]

  summDT[total_dollars <= dollars_low, vip_dollars := 0]
  summDT[total_dollars > dollars_low & total_dollars <= dollars_med, vip_dollars := 1]
  summDT[total_dollars > dollars_med & total_dollars <= dollars_high, vip_dollars := 2]
  summDT[total_dollars > dollars_high, vip_dollars := 3]

  # Drop these aux columns.
  drop_cols <- names(summDT)[stringr::str_detect(names(summDT), "low|med|high")]
  for (d in drop_cols) {
    set(summDT, NULL, d, NULL)
  }
  summDT[, vip_points := apply(
    summDT[, summDT[, stringr::str_detect(names(.SD), "vip")], with = FALSE], 1, sum
  )]
  setkeyv(summDT, c("org_uuid", "customer_id"))
  summDT[]
}
