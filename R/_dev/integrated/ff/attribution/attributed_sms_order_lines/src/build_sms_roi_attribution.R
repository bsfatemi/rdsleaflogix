# Marketing Studio App Data
# build_sms_roi_attribution
#
# (C) 2020 Happy Cabbage Analytics Inc.

box::use(
  dplyr[
    mutate, filter, coalesce, distinct, transmute, inner_join, group_by, arrange,
    `%>%`, left_join, bind_rows, rename, ungroup, lag
  ],
  pipelinetools[process_phone],
  lubridate[with_tz, date, ymd_hms, hours, days, weeks],
  stringr[str_squish, str_replace_all],
  tidyr[fill]
)


build_sms_roi_attribution <- function(sms, campaigns, population) {
  # Extract Sample to Evaluate ROI
  phones <- population %>%
    mutate(
      phone = process_phone(phone)
    ) %>%
    filter(coalesce(phone, "") != "") %>%
    distinct(
      customer_id, org, source_system, phone
    )

  # GET ORDER INFO
  order_info <- population %>%
    transmute(
      customer_id,
      org,
      source_system,
      order_id,
      order_type,
      order_facility,
      event_time_utc = ymd_hms(order_time_utc, tz = "UTC"),
      order_time_local,
      order_total,
      event = "order"
    ) %>%
    distinct() %>%
    inner_join(phones, by = c("customer_id", "org", "source_system")) %>%
    group_by(org, customer_id) %>%
    arrange(order_time_local) %>%
    mutate(
      order_date_numeric = as.numeric(date(order_time_local)),
      time_since_last_order = order_date_numeric - lag(order_date_numeric, 1)
    )

  # GET SMS INFO
  clean_bodies <- sms %>%
    left_join(campaigns, by = c("org", "campaign_id")) %>%
    mutate(
      body = str_squish(str_replace_all(coalesce(campaign_body, body), "\n", " "))
    )
  sms <- NULL

  sms <- clean_bodies %>%
    inner_join(phones, by = c("to" = "phone", "org" = "org")) %>%
    transmute(
      org,
      campaign_id,
      customer_id,
      phone = to,
      body,
      date_sent_utc = with_tz(date_sent, "UTC"),
      event_time_utc = date_sent_utc,
      event = "sms"
    )

  # STACK
  stacked <- bind_rows(order_info, sms)

  # Find last touch
  last_touch_attributed <- stacked %>%
    group_by(customer_id, org) %>%
    arrange(event_time_utc) %>%
    fill(date_sent_utc) %>%
    rename(
      last_sms_date_sent = date_sent_utc,
      last_sms_body = body,
      last_sms_campaign_id = campaign_id
    ) %>%
    fill(last_sms_date_sent, last_sms_body, last_sms_campaign_id) %>%
    filter(event == "order") %>%
    ungroup()

  # Measure ROI
  #
  # Pre-Built Increments
  # 24 hour
  # 72 hour
  # 7 day
  # 2 week
  # 30 day

  orders_roi <- last_touch_attributed %>%
    filter(!is.na(last_sms_body)) %>%
    mutate(
      is_24hr  = event_time_utc <= last_sms_date_sent + hours(24),
      is_72hr  = event_time_utc <= last_sms_date_sent + hours(72),
      is_7day  = event_time_utc <= last_sms_date_sent + days(7),
      is_2wk   = event_time_utc <= last_sms_date_sent + weeks(2),
      is_30day = event_time_utc <= last_sms_date_sent + days(30),
      was_lost = time_since_last_order >= 90
    )

  order_lines <- population %>%
    distinct(
      org, source_system, order_id, order_line_id,
      product_name, product_id, brand_name, product_uom,
      product_unit_count, product_qty,
      product_category_name, category3, product_class, order_line_total
    )

  product_roi <- inner_join(orders_roi, order_lines, by = c("org", "source_system", "order_id"))

  return(product_roi)
}
