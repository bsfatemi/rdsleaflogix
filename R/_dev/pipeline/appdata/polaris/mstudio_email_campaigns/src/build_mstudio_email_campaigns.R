# Marketing Studio Campaigns
# build_mstudio_email_campaigns.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

box::use(
  dplyr[coalesce, distinct, group_by, if_else, left_join, mutate, n_distinct, summarise],
  stringr[str_replace_all, str_squish],
  lubridate[now]
)

build_mstudio_email_campaigns <- function(campaign_info, sent_emails_info,
                                          email_attributed_order_lines) {
  # Clean text fields.
  campaign_info <- left_join(
    campaign_info, sent_emails_info,
    by = c(archx_campaign_id = "campaign_id")
  ) |>
    mutate(
      customers_reached = coalesce(customers_reached, 0),
      delivery_rate = coalesce(delivery_rate, 0),
      open_rate = coalesce(open_rate, 0), click_rate = coalesce(click_rate, 0),
      email_subject = str_squish(str_replace_all(email_subject, "\n", " ")),
      email_body = str_squish(str_replace_all(email_body, "\n", " ")),
      text_length = nchar(email_subject) + nchar(email_body)
    )

  # Aggregate ROI information.
  campaign_roi <- distinct(
    email_attributed_order_lines,
    campaign_uuid = last_email_campaign_uuid, customer_id, order_total,
    is_24hr, is_72hr, is_7day, is_2wk, is_30day, was_lost
  ) |>
    group_by(campaign_uuid) |>
    summarise(
      # ROI.
      x24hr_roi = sum(order_total * is_24hr, na.rm = TRUE),
      x72hr_roi = sum(order_total * is_72hr, na.rm = TRUE),
      x7day_roi = sum(order_total * is_7day, na.rm = TRUE),
      x2wk_roi = sum(order_total * is_2wk, na.rm = TRUE),
      x30day_roi = sum(order_total * is_30day, na.rm = TRUE),
      # Customers.
      x24hr_customers = n_distinct(if_else(is_24hr, customer_id, as.character(NA)), na.rm = TRUE),
      x72hr_customers = n_distinct(if_else(is_72hr, customer_id, as.character(NA)), na.rm = TRUE),
      x7day_customers = n_distinct(if_else(is_7day, customer_id, as.character(NA)), na.rm = TRUE),
      x2wk_customers = n_distinct(if_else(is_2wk, customer_id, as.character(NA)), na.rm = TRUE),
      x30day_customers = n_distinct(if_else(is_30day, customer_id, as.character(NA)), na.rm = TRUE),
      # Recapture.
      x24hr_recapture = n_distinct(
        if_else(is_24hr & was_lost, customer_id, as.character(NA)),
        na.rm = TRUE
      ),
      x72hr_recapture = n_distinct(
        if_else(is_72hr & was_lost, customer_id, as.character(NA)),
        na.rm = TRUE
      ),
      x7day_recapture = n_distinct(
        if_else(is_7day & was_lost, customer_id, as.character(NA)),
        na.rm = TRUE
      ),
      x2wk_recapture = n_distinct(
        if_else(is_2wk & was_lost, customer_id, as.character(NA)),
        na.rm = TRUE
      ),
      x30day_recapture = n_distinct(
        if_else(is_30day & was_lost, customer_id, as.character(NA)),
        na.rm = TRUE
      ),
      # 24 hr recapture
      x24hr_recapture_sales = sum(is_24hr * was_lost * order_total, na.rm = TRUE),
      x72hr_recapture_sales = sum(is_72hr * was_lost * order_total, na.rm = TRUE),
      x7day_recapture_sales = sum(is_7day * was_lost * order_total, na.rm = TRUE),
      x2wk_recapture_sales = sum(is_2wk * was_lost * order_total, na.rm = TRUE),
      x30day_recapture_sales = sum(is_30day * was_lost * order_total, na.rm = TRUE),
      .groups = "drop"
    )

  campaigns <- left_join(campaign_info, campaign_roi, by = "campaign_uuid")
  # Record when DB write occurs.
  campaigns$run_date_utc <- now("UTC")
  return(campaigns)
}
