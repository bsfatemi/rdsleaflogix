box::use(
  dplyr[filter, group_by, if_else, mutate, mutate_at, summarise, vars],
  stats[quantile]
)

build_loyalty_benchmark <- function(oid, odt) {
  print(paste0("getting org: ", oid))
  if (nrow(odt) == 0) {
    print("no data found... returning null")
    return(NULL)
  } else {
    filter(odt, is.finite(item_total), is.finite(item_discount), is.finite(product_qty)) |>
      group_by(org_uuid = oid, customer_id) |>
      summarise(
        spend_by_customer = sum(item_total, na.rm = TRUE),
        qty_by_customer = sum(product_qty, na.rm = TRUE),
        orders_by_customer = length(unique(order_id)),
        discount_by_customer = abs(sum(item_discount, na.rm = TRUE)),
        .groups = "drop"
      ) |>
      mutate(avg_discount_by_customer = discount_by_customer / spend_by_customer) |>
      group_by(org_uuid) |>
      summarise(
        # Total spend by customer.
        spend_silver = quantile(spend_by_customer, .45),
        spend_gold = quantile(spend_by_customer, .70),
        spend_platinum = quantile(spend_by_customer, .95),
        # Total items purchased by customer.
        items_silver = quantile(qty_by_customer, .50),
        items_gold = quantile(qty_by_customer, .85),
        items_platinum = quantile(qty_by_customer, .98),
        # Total orders by customer.
        orders_silver = quantile(orders_by_customer, .45),
        orders_gold = quantile(orders_by_customer, .70),
        orders_platinum = quantile(orders_by_customer, .85),
        # Total average discount by customer.
        disc_silver = quantile(
          if_else(avg_discount_by_customer > 0.01, avg_discount_by_customer, NA_real_),
          .25,
          na.rm = TRUE
        ),
        disc_gold = quantile(
          if_else(avg_discount_by_customer > 0.01, avg_discount_by_customer, NA_real_),
          .60,
          na.rm = TRUE
        ),
        disc_platinum = quantile(
          if_else(avg_discount_by_customer > 0.01, avg_discount_by_customer, NA_real_),
          .95,
          na.rm = TRUE
        )
      ) |>
      mutate_at(vars(-org_uuid), ~ if_else(is.na(.x), 0, .x))
  }
}
