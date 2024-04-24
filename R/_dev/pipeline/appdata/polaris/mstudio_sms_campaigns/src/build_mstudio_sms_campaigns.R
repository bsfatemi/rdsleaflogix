# Marketing Studio Campaigns
# build_mstudio_sms_campaigns.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

box::use(
  dplyr[bind_rows, coalesce, distinct, filter, group_by, if_else,
        left_join, mutate, n_distinct, pull, summarize, ungroup],
  stringr[str_replace_all, str_squish, str_to_sentence],
  lubridate[date, now, year],
  janeaustenr[austen_books]
)


build_mstudio_sms_campaigns <- function(sms_msgs, campaign_info,
                                        opt_out_info, sms_attributed_order_lines,
                                        polaris_config) {
  clean_bodies <- sms_msgs %>%
    left_join(campaign_info, by = c("campaign_id", "org")) %>%
    mutate(
      text_length = nchar(coalesce(campaign_body, body)),
      body = str_squish(str_replace_all(coalesce(campaign_body, body), "\n", " ")),
    )

  sms_msgs <- NULL

  # SMS Campaign Info like Opt-Outs, Texts Delivered, Text Length, Etc.
  info <- clean_bodies %>%
    filter(sent_from_messaging_service == TRUE, !is.na(campaign_id)) %>%
    group_by(campaign_id, org, is_mms) %>%
    summarize(
      customers = n_distinct(if_else(is.na(error_code), to, as.character(NA)), na.rm = TRUE),
      texts = sum(if_else(is.na(error_code), as.numeric(num_segments), 0), na.rm = TRUE),
      texts_sent = sum(as.numeric(num_segments), na.rm = TRUE),
      customers_targeted = n_distinct(to),
      first_text = min(date_sent, na.rm = TRUE),
      last_text = max(date_sent, na.rm = TRUE),
      days = n_distinct(date(date_sent)),
      price = sum(as.numeric(price), na.rm = TRUE),
      text_length = max(text_length, na.rm = TRUE)
    ) %>%
    left_join(polaris_config, by = "org") %>%
    mutate(billed_text = if_else(is_mms, mms_ratio * texts, texts)) %>%
    group_by(campaign_id, org) %>%
    summarize(
      customers = sum(as.numeric(customers), na.rm = TRUE),
      texts = sum(as.numeric(texts), na.rm = TRUE),
      texts_sent = sum(as.numeric(texts_sent), na.rm = TRUE),
      customers_targeted = sum(as.numeric(customers_targeted), na.rm = TRUE),
      first_text = min(first_text, na.rm = TRUE),
      last_text = max(last_text, na.rm = TRUE),
      days = max(days, na.rm = TRUE),
      price = sum(as.numeric(price), na.rm = TRUE),
      text_length = max(text_length, na.rm = TRUE),
      is_mms = any(is_mms == "TRUE"),
      billed_texts = sum(as.numeric(billed_text), na.rm = TRUE)
    ) %>%
    mutate(
      cost_per_customer = price / customers,
      is_single_text = customers < 3,
      is_many_days = days > 2,
      pct_delivered = texts / texts_sent,
      pct_reached = customers / customers_targeted
    ) %>%
    left_join(opt_out_info, by = c("campaign_id", "org")) %>%
    mutate(
      opt_out_rate = as.double(unsubscribes) / as.double(customers)
    ) %>%
    left_join(rename(campaign_info, "body" = "campaign_body"), by = c("campaign_id", "org"))

  info$updated_at <- now(tzone = "UTC")
  clean_bodies <- NULL

  # AGGREGATE ROI INFORMATION
  campaign_roi <- sms_attributed_order_lines %>%
    distinct(
      campaign_id = last_sms_campaign_id, org,
      customer_id, order_total,
      is_24hr, is_72hr, is_7day, is_2wk, is_30day, was_lost
    ) %>%
    group_by(campaign_id, org) %>%
    summarize(
      # ROI
      x24hr_roi = sum(order_total * is_24hr, na.rm = TRUE),
      x72hr_roi = sum(order_total * is_72hr, na.rm = TRUE),
      x7day_roi = sum(order_total * is_7day, na.rm = TRUE),
      x2wk_roi = sum(order_total * is_2wk, na.rm = TRUE),
      x30day_roi = sum(order_total * is_30day, na.rm = TRUE),

      # Customers
      x24hr_customers = n_distinct(if_else(is_24hr, customer_id, as.character(NA)), na.rm = TRUE),
      x72hr_customers = n_distinct(if_else(is_72hr, customer_id, as.character(NA)), na.rm = TRUE),
      x7day_customers = n_distinct(if_else(is_7day, customer_id, as.character(NA)), na.rm = TRUE),
      x2wk_customers = n_distinct(if_else(is_2wk, customer_id, as.character(NA)), na.rm = TRUE),
      x30day_customers = n_distinct(if_else(is_30day, customer_id, as.character(NA)), na.rm = TRUE),

      # Recapture
      x24hr_recapture = n_distinct(
        if_else(is_24hr & was_lost, customer_id, as.character(NA)), na.rm = TRUE
      ),
      x72hr_recapture = n_distinct(
        if_else(is_72hr & was_lost, customer_id, as.character(NA)), na.rm = TRUE
      ),
      x7day_recapture = n_distinct(
        if_else(is_7day & was_lost, customer_id, as.character(NA)), na.rm = TRUE
      ),
      x2wk_recapture = n_distinct(
        if_else(is_2wk & was_lost, customer_id, as.character(NA)), na.rm = TRUE
      ),
      x30day_recapture = n_distinct(
        if_else(is_30day & was_lost, customer_id, as.character(NA)), na.rm = TRUE
      ),

      # 24 hr recapture
      x24hr_recapture_sales = sum(is_24hr * was_lost * order_total, na.rm = TRUE),
      x72hr_recapture_sales = sum(is_72hr * was_lost * order_total, na.rm = TRUE),
      x7day_recapture_sales = sum(is_7day * was_lost * order_total, na.rm = TRUE),
      x2wk_recapture_sales = sum(is_2wk * was_lost * order_total, na.rm = TRUE),
      x30day_recapture_sales = sum(is_30day * was_lost * order_total, na.rm = TRUE)
    ) %>%
    ungroup()

  campaigns <- info %>%
    left_join(campaign_roi, by = c("campaign_id", "org")) %>%
    filter(org != "demo")

  # MAKE DEMO VERSION
  demo <- data.frame()
  if (polaris_config$org == "medithrive") {
    # TODO: Andrew, you should probably do this in a more robust way.
    `_build_demo_campaigns` <- function(campaigns) {
      demo_sample <- austen_books() %>%
        filter(text != "", nchar(text) > 50) %>%
        pull(text) %>%
        str_to_sentence()
      demo_campaigns <- campaigns %>%
        filter(org == "medithrive", year(first_text) >= 2020)
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
    demo <- `_build_demo_campaigns`(campaigns)
  }
  output <- bind_rows(campaigns, demo)

  # Record when DB write occurs
  run_date_utc <- now(tzone = "UTC")
  output$run_date_utc <- run_date_utc

  return(output)
}
