# Polaris - Agg. Campaigns
# build_mstudio_product_roi.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

box::use(
  dplyr[distinct, group_by, summarize, ungroup, bind_rows, pull, mutate, filter, `%>%`],
  janeaustenr[austen_books],
  stringr[str_to_sentence]
)

build_mstudio_product_roi <- function(sms_attributed_order_lines) {

  # Data aggregation
  roi <- sms_attributed_order_lines %>%
    distinct() %>%
    group_by(
      campaign_id = last_sms_campaign_id,
      body = last_sms_body,
      product_name,
      product_category_name,
      category3,
      brand_name,
      org
    ) %>%
    summarize(
      # ROI
      x24hr_roi  = sum(order_line_total * is_24hr, na.rm = TRUE),
      x72hr_roi  = sum(order_line_total * is_72hr, na.rm = TRUE),
      x7day_roi  = sum(order_line_total * is_7day, na.rm = TRUE),
      x2wk_roi   = sum(order_line_total * is_2wk,  na.rm = TRUE),
      x30day_roi = sum(order_line_total * is_30day, na.rm = TRUE),
      # Orders
      x24hr_orders  = sum(as.integer(is_24hr), na.rm = TRUE),
      x72hr_orders  = sum(as.integer(is_72hr), na.rm = TRUE),
      x7day_orders  = sum(as.integer(is_7day), na.rm = TRUE),
      x2wk_orders   = sum(as.integer(is_2wk), na.rm = TRUE),
      x30day_orders = sum(as.integer(is_30day), na.rm = TRUE),
      .groups = "drop"
    )

  # MAKE DEMO VERSION
  # TODO: Andrew, you should probably do this in a more robust way.
  `_build_demo_campaigns` <- function(campaigns) {
    demo_sample <- austen_books() %>%
      filter(text != "", nchar(text) > 50) %>%
      pull(text) %>%
      str_to_sentence()
    demo_campaigns <- filter(campaigns, org == "medithrive")
    anonymized <- demo_campaigns %>%
      ungroup() %>%
      mutate(
        body = paste(
          sample(demo_sample, size = nrow(.), replace = TRUE),
          sample(demo_sample, size = nrow(.), replace = TRUE)
        ),
        org  = "demo"
      )
    return(anonymized)
  }
  demo <- `_build_demo_campaigns`(roi)
  output <- bind_rows(roi, demo)

  # Record when DB write occurs
  output$run_date_utc <- lubridate::now(tzone = "UTC")

  return(output)
}
