box::use(
  DBI[dbGetQuery],
  dplyr[
    arrange, bind_rows, count, desc, distinct, filter, group_by, group_map, inner_join, lead,
    left_join, mutate, n, n_distinct, select, summarise, ungroup
  ],
  glue[glue],
  lubridate[days, now, today],
  tidyr[pivot_wider]
)

read_org_pop <- function(pg_in, org_uuid) {
  # Read the org's population2 data.
  org_pop <- dbGetQuery(pg_in, glue("
    SELECT *
    FROM population2
    WHERE org_uuid = '{org_uuid}' AND phone IS NOT NULL AND customer_id IS NOT NULL
  "))
}

build_customer_profile <- function(org_pop) {
  # Initial filter - Only for Customers with more than 3 orders
  org_pop <- group_by(org_pop, customer_id, phone) |>
    mutate(total_orders = n_distinct(order_id)) |>
    ungroup() |>
    filter(total_orders > 3)
  # Build the customer profile.
  customer_profile <- distinct(org_pop, customer_id, phone, order_id, .keep_all = TRUE) |>
    arrange(desc(order_time_utc)) |>
    group_by(customer_id, phone) |>
    mutate(
      prev_order_time_utc = lead(order_time_utc),
      days_since_prev_purchase = as.numeric(difftime(
        order_time_utc, prev_order_time_utc, units = "days"
      )),
    ) |>
    summarise(
      last_purchase_date = max(order_time_utc),
      days_since_last_purchase = as.numeric(difftime(now(), max(order_time_utc), units = "days")),
      ave_days_between = mean(days_since_prev_purchase, na.rm = TRUE),
      sd_days_between = sd(days_since_prev_purchase, na.rm = TRUE),
      med_order_total = median(order_tot),
      tot_orders = n(),
      tot_dollars = sum(order_tot),
      .groups = "drop"
    ) |>
    filter(!is.na(sd_days_between)) |>
    # Mark customer as lost when its been at least 5 STDEV away in days since last order
    mutate(
      cust_is_lost = days_since_last_purchase > ave_days_between + (sd_days_between * 5)
    )
  # Consider only non-lost customers for discount senstivity model and preference profile models
  disc_sens <- filter(customer_profile, !cust_is_lost) |>
    select(customer_id, phone) |>
    inner_join(org_pop, by = c("customer_id", "phone")) |>
    distinct(customer_id, phone, order_id, .keep_all = TRUE) |>
    filter(order_disc < 0)
  # For each customer, calc model output.
  # If discounts have a statistically sign. impact on order total for a non-lost (regular)
  # customer, then that customer is Discount Sensitive.
  if (nrow(disc_sens) > 0) {
    disc_sens <- group_by(disc_sens, customer_id, phone) |>
      group_map(function(customer_disc_sens, customer_index) {
        mlsum <- summary(lm(order_subtot ~ order_disc, customer_disc_sens))$coefficients
        is_disc_sensitive <- nrow(mlsum) > 1 && !is.na(mlsum[2, 4]) && mlsum[2, 4] < .15
        customer_index$is_disc_sensitive <- is_disc_sensitive
        customer_index
      }) |>
      bind_rows()
  } else {
    disc_sens <- data.frame(
      customer_id = character(), phone = character(), is_disc_sensitive = logical()
    )
  }
  customer_profile <- left_join(customer_profile, disc_sens, by = c("customer_id", "phone"))
  customer_profile <- mutate(
    customer_profile,
    org_uuid = !!unique(org_pop$org_uuid),
    fac = 2.65 * sd_days_between / sqrt(tot_orders),
    days_start = pmax(floor(ave_days_between - fac), 0),
    days_stop = floor(ave_days_between + fac),
    next_start_utc = today("UTC") + days(days_start),
    next_stop_utc = today("UTC") + days(days_stop)
  ) |>
    select(-fac, -days_start, -days_stop)
  # Get customers' fav brand per category.
  customer_favs <- count(
    org_pop, customer_id, phone, category2, brand_name, wt = item_total, sort = TRUE
  ) |>
    distinct(customer_id, phone, category2, .keep_all = TRUE) |>
    filter(!is.na(category2)) |>
    mutate(category2 = tolower(category2)) |>
    select(-n) |>
    pivot_wider(names_from = category2, values_from = brand_name)
  left_join(customer_profile, customer_favs, by = c("customer_id", "phone"))
}
