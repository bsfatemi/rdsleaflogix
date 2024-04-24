# Polaris - Agg. Campaigns
# build_mstudio_product_roi.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

box::use(
  dplyr[distinct, group_by, summarise],
  lubridate[now]
)

#' @export
build_mstudio_email_product_roi <- function(attr_lines) {
  # Data aggregation.
  roi <- distinct(attr_lines) |>
    group_by(
      campaign_id = last_email_campaign_uuid, subject = last_email_subject, body = last_email_body,
      img_path = last_email_img_path, template_id = last_email_template_id, product_name,
      product_category_name, brand_name
    ) |>
    summarise(
      # ROI.
      x24hr_roi = sum(order_line_total * is_24hr, na.rm = TRUE),
      x72hr_roi = sum(order_line_total * is_72hr, na.rm = TRUE),
      x7day_roi = sum(order_line_total * is_7day, na.rm = TRUE),
      x2wk_roi = sum(order_line_total * is_2wk, na.rm = TRUE),
      x30day_roi = sum(order_line_total * is_30day, na.rm = TRUE),
      # Orders.
      x24hr_orders = sum(as.integer(is_24hr), na.rm = TRUE),
      x72hr_orders = sum(as.integer(is_72hr), na.rm = TRUE),
      x7day_orders = sum(as.integer(is_7day), na.rm = TRUE),
      x2wk_orders = sum(as.integer(is_2wk), na.rm = TRUE),
      x30day_orders = sum(as.integer(is_30day), na.rm = TRUE),
      .groups = "drop"
    )

  # Record when DB write occurs.
  roi$run_date_utc <- now("UTC")
  return(roi)
}
