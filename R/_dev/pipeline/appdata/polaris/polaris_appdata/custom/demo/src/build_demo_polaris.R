# Marketing Studio App Data
# build_demo_polaris.R
#
# (C) Happy Cabbage Analytics, Inc. 2021
build_demo_polaris <- function(order_info, raw_customers) {
  # GET DEMOGRAPHICS
  customer_info <- raw_customers %>%
    transmute(
      customer_id,
      source_system       = source_system,
      org                 = org,
      updated_at          = ymd_hms(last_updated_utc),
      last_location_info  = long_address,
      last_customer_phone = phone,
      last_customer_email = email,
      last_locality       = city,
      last_customer_name  = full_name,
      last_state          = state,
      last_zipcode        = zipcode,
      last_opted_in       = if_else(is_subscribed, "Y", "N"),
      last_lat            = latitude,
      last_lon            = longitude,
      last_sms            = last_sms_engagement_utc,
      tot_sms             = tot_sms_received,
      age                 = sample(age, size = nrow(.), replace = TRUE),
      gender              = sample(c("M", "F"), size = nrow(.), replace = TRUE),
      user_type           = user_type
    )
  customers <- distinct(inner_join(customer_info, order_info))


  # replacement data
  phone_nums <- c(
    "+14436689972", "+17032329564", "+16507877901", "+18583612974", "+15097011164", "+16105853314"
  )
  groups <- c("Veterans", "Students", "VIP Club", "Neighbors")
  addresses <- c(
    "123 Main St", "4 Privet Drive", "718A Clementina St", "221B Baker Street", "631 Folsom St",
    "10 Downing St", "12 Grimmauld Place"
  )
  as_of_date <- today()
  days_to_add <- interval(max(customers$last_order_date, na.rm = TRUE), as_of_date) %/% days(1)

  test_data <- customers %>%
    filter(str_to_upper(last_locality) %in% c(
      "SAN FRANCISCO",
      "SOUTH SAN FRANCISCO", "DALY CITY",
      "SAN JOSE", "OAKLAND", "FREMONT", "BERKELEY",
      "SAN MATEO", "ANTIOCH", "DUBLIN", "CONCORD", "WALNUT CREEK",
      "HAYWARD", "PALO ALTO", "MOUNTAIN VIEW", "RICHMOND",
      "ALAMEDA", "REDWOOD CITY", "PLEASANTON", "LIVERMORE"
    )) %>%
    mutate(
      last_customer_name = r_full_names(n = nrow(.)),
      first_name = word(last_customer_name, 1),
      last_name = word(last_customer_name, 2),
      org = "demo",
      last_customer_email = r_email_addresses(n = nrow(.)),
      last_order_source = coalesce(last_order_source, "Unknown"),
      last_customer_phone = sample(phone_nums, size = nrow(.), replace = TRUE),
      last_location_info = sample(addresses, size = nrow(.), replace = TRUE),
      last_opted_in = "Y",
      tags = sample(groups, size = nrow(.), replace = TRUE),
      loyalty_pts = sample(c(1:500), size = nrow(.), replace = TRUE),
      tot_sms = tot_sms + 1,
      last_sms = format(coalesce(ymd_hms(last_sms), now()) - days(7), "%Y-%m-%d %H:%M:%S"),
      last_order_date = ymd_hms(last_order_date) + days(days_to_add),
      first_order_date = ymd_hms(first_order_date) + days(days_to_add),
      last_order_facility = case_when(
        str_to_upper(last_locality) %in% c(
          "SAN FRANCISCO", "DALY CITY", "SAN MATEO", "SOUTH SAN FRANCISCO"
        ) ~ "San Francisco / Penninsula",
        str_to_upper(last_locality) %in% c(
          "OAKLAND", "FREMONT", "BERKELEY", "HAYWARD", "RICHMOND", "ALAMEDA"
        ) ~ "East Bay",
        str_to_upper(last_locality) %in% c(
          "DUBLIN", "WALNUT CREEK", "ANTIOCH", "CONCORD", "PLEASANTON", "LIVERMORE"
        ) ~ "Far East Bay",
        TRUE ~ "South Bay"
      ),
      updated_at = now()
    )

  return(test_data)
}
