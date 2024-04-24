build_customer_order_summaries <- function(population) {
  setDT(population)
  cos <- population[
    !(is.na(phone) & is.na(customer_id)) # Drop if customer does not have ID and phone.
  ][
    !is.na(item_total) & !is.na(product_qty)
  ][
    , classification2 := stringr::str_to_upper(classification2)
  ][
    ,
    .(
      last_order_utc = max(order_time_utc, na.rm = TRUE),
      sales_last_order = sum(item_total[order_time_utc == max(order_time_utc, na.rm = TRUE)]),
      sales_all_time = sum(item_total),
      product_qty = sum(product_qty)
    ),
    keyby = .(org_uuid, customer_id, phone, product_name, brand_name, category2, classification2)
  ]

  setkeyv(cos, c("org_uuid", "customer_id"))
  cos[]
}
