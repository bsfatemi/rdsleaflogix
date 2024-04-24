# Marketing Studio App Data
# build_diem_mstudio.R
#
# (C) 2020 Happy Cabbage Analytics Inc.

build_diem_mstudio <- function(raw_customers, order_info) {
  # GET DEMOGRAPHICS
  customer_info <- raw_customers %>%
    transmute(
      customer_id,
      org,
      source_system       = source_system,
      updated_at          = ymd_hms(last_updated_utc),
      last_location_info  = long_address,
      last_customer_phone = phone,
      last_customer_email = email,
      last_locality       = city,
      first_name,
      last_name,
      last_customer_name  = full_name,
      last_state          = state,
      last_zipcode        = zipcode,
      last_opted_in       = if_else(is_subscribed, "Y", "N"),
      last_lat            = latitude,
      last_lon            = longitude,
      last_sms            = last_sms_engagement_utc,
      tot_sms             = tot_sms_received,
      age                 = age,
      gender              = gender,
      user_type           = user_type
    )

  # Fill in information for customers who have never ordered
  output <- customer_info %>%
    left_join(order_info) %>%
    mutate(
      last_order_facility = coalesce(last_order_facility, "Never Ordered"),
      last_order_source = coalesce(last_order_source, "Unknown")
    ) %>%
    mutate_at(vars(last_order_date, first_order_date), ~ coalesce(., updated_at)) %>%
    mutate_at(
      vars(total_purchases_usd, avg_time_between_orders, avg_order_size, pct_delivery),
      ~ coalesce(., 0)
    ) %>%
    mutate_at(vars(num_orders), ~ coalesce(., 0L)) %>%
    mutate_at(vars(
      total_purchases_usd, avg_time_between_orders,
      avg_order_time, avg_order_size,
      pct_flower, pct_vapes, pct_other, pct_topical, pct_extract,
      pct_preroll, pct_edible, pct_drinks, pct_tinctures, pct_tablets_capsules,
      pct_hybrid, pct_indica, pct_none, pct_sativa, pct_cbd
    ), ~ coalesce(., 0)) %>%
    distinct()

  return(output)
}
