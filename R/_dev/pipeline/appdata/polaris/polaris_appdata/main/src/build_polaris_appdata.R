# Marketing Studio App Data
#
# (C) 2020 Happy Cabbage Analytics Inc.

box::use(
  dplyr[
    mutate, group_by, filter, arrange, ungroup, row_number, mutate_at,
    vars, transmute, left_join, coalesce, distinct, select, case_when
  ],
  lubridate[as_date, ymd_hms, today],
  pipelinetools[process_phone],
  hcaconfig[orgIndex]
)

build_polaris_appdata <- function(customers, customer_behavior, optins, abandoned_carts) {

  abandoned_carts <- abandoned_carts |>
    filter(!is.na(phone)) |>
    # Group by phone and filter to most recent to prevent any duplicate customer records
    group_by(phone) |>
    filter(as_date(created_at) == max(as_date(created_at))) |>
    ungroup() |>
    transmute(
      phone = process_phone(phone),
      days_since_cart = as.integer(today() - as_date(created_at)),
      orguuid
    ) |>
    distinct()

  # GET DEMOGRAPHICS
  customers <- customers |>
    filter(!is.na(customer_id), !is.na(last_updated_utc)) |>
    left_join(select(orgIndex(), org = short_name, orguuid = org_uuid), by = "org") |>
    left_join(optins, by = c("phone", "orguuid")) |>
    left_join(abandoned_carts, by = c("phone", "orguuid")) |>
    transmute(
      customer_id,
      org,
      age,
      birthday,
      gender,
      user_type,
      updated_at = ymd_hms(last_updated_utc),
      last_location_info = long_address,
      last_customer_phone = phone,
      last_customer_email = email,
      last_locality = city,
      first_name,
      last_name,
      last_customer_name = full_name,
      last_state = state,
      last_zipcode = zipcode,
      last_opted_in = case_when(
        status == "opt_in" ~ "Y",
        status == "opt_out" ~ "N",
        pos_is_subscribed == FALSE ~ "N",
        pos_is_subscribed == TRUE ~ "Y",
        TRUE ~ "N"
      ),
      last_lat = latitude,
      last_lon = longitude,
      last_sms = last_sms_engagement_utc,
      tot_sms = tot_sms_received,
      loyalty_pts,
      tags,
      days_since_cart
    )

  output <- customers |>
    left_join(customer_behavior, by = c("customer_id", "org")) |>
    mutate(last_order_facility = coalesce(last_order_facility, "Never Ordered"),
           last_order_source = coalesce(last_order_source, "Unknown")) |>
    mutate_at(vars(last_order_date, first_order_date), ~ coalesce(., updated_at)) |>
    mutate_at(
      vars(total_purchases_usd, avg_time_between_orders, avg_order_size, pct_delivery),
      ~ coalesce(., 0)
    ) |>
    mutate_at(vars(num_orders), ~ coalesce(., 0L)) |>
    mutate_at(vars(
      total_purchases_usd, avg_time_between_orders,
      avg_order_time, avg_order_size,
      pct_flower, pct_vapes, pct_other, pct_topical, pct_extract,
      pct_preroll, pct_edible, pct_drinks, pct_tinctures, pct_tablets_capsules,
      pct_hybrid, pct_indica, pct_none, pct_sativa, pct_cbd
    ), ~ coalesce(., 0)) |>
    distinct()

  return(output)
}
