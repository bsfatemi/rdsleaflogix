# Marketing Studio App Data
# build_email_roi_attribution
#
# (C) 2020 Happy Cabbage Analytics Inc.

box::use(
  dplyr[
    arrange, bind_rows, distinct, filter, group_by, inner_join, lag, left_join, mutate, rename,
    transmute, ungroup
  ],
  lubridate[date, days, hours, weeks, ymd_hms],
  stringr[str_replace_all, str_squish],
  tidyr[fill]
)

#' @export
build_email_roi_attribution <- function(sent_emails, campaigns, population) {
  emails <- filter(population, !is.na(email)) |>
    distinct(customer_id, org, source_system, email)

  # Get order info.
  order_info <- transmute(
    population,
    customer_id, org, source_system, order_id, order_type, order_facility,
    event_time_utc = ymd_hms(order_time_utc, tz = "UTC"), order_time_local, order_total,
    event = "order"
  ) |>
    distinct() |>
    inner_join(emails, by = c("customer_id", "org", "source_system")) |>
    group_by(org, customer_id) |>
    arrange(order_time_local) |>
    mutate(
      order_date_numeric = as.numeric(date(order_time_local)),
      time_since_last_order = order_date_numeric - lag(order_date_numeric, 1)
    )

  # Get emails info.
  clean_bodies <- left_join(sent_emails, campaigns, by = c(campaign_id = "archx_campaign_id")) |>
    mutate(
      subject = str_squish(str_replace_all(email_subject, "\n", " ")),
      body = str_squish(str_replace_all(email_body, "\n", " ")),
      img_path = email_img_path, template_id = email_template_id
    )
  sent_emails <- inner_join(clean_bodies, emails, by = "email") |>
    transmute(
      org, campaign_uuid, customer_id, email, subject, body, img_path, template_id,
      sent_at_utc, event_time_utc = sent_at_utc, event = "email"
    )

  # Find last touch.
  last_touch_attributed <- bind_rows(order_info, sent_emails) |>
    group_by(customer_id, org) |>
    arrange(event_time_utc) |>
    fill(sent_at_utc) |>
    rename(
      last_email_date_sent = sent_at_utc,
      last_email_subject = subject,
      last_email_body = body,
      last_email_img_path = img_path,
      last_email_template_id = template_id,
      last_email_campaign_uuid = campaign_uuid
    ) |>
    fill(
      last_email_date_sent, last_email_subject, last_email_body, last_email_img_path,
      last_email_template_id, last_email_campaign_uuid
    ) |>
    filter(event == "order") |>
    ungroup()

  # Measure ROI.
  orders_roi <- filter(last_touch_attributed, !is.na(last_email_body)) |>
    mutate(
      is_24hr  = event_time_utc <= last_email_date_sent + hours(24),
      is_72hr  = event_time_utc <= last_email_date_sent + hours(72),
      is_7day  = event_time_utc <= last_email_date_sent + days(7),
      is_2wk   = event_time_utc <= last_email_date_sent + weeks(2),
      is_30day = event_time_utc <= last_email_date_sent + days(30),
      was_lost = time_since_last_order >= 90
    )

  order_lines <- distinct(
    population,
    org, source_system, order_id, order_line_id, product_name, product_id, brand_name, product_uom,
    product_unit_count, product_qty, product_category_name, product_class, order_line_total
  )
  product_roi <- inner_join(orders_roi, order_lines, by = c("org", "source_system", "order_id"))
  return(product_roi)
}
