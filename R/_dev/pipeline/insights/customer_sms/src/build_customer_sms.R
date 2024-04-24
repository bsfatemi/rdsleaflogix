# Consolidated Customers
# build_customer_sms.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  dplyr[
    `%>%`, arrange, filter, group_by, if_else, lead, left_join, mutate, mutate_at, row_number,
    select, transmute, ungroup, vars
  ],
  lubridate[as_datetime, with_tz],
  stringr[str_detect, str_replace_all, str_squish, str_to_lower],
  tidyr[fill]
)

build_customer_sms <- function(sms) {
  # Opt-out keywords
  stop_keywords <- c("stop", "unsubscribe", "cancel", "remove", "end", "quit", "stoo", "fuck off")
  start_keywords <- c("start", "yes", "unstop")

  # Campaigns
  sms <- mutate(sms, body = str_squish(str_replace_all(body, "\n", " ")))

  # What did the send
  from_customers <- sms %>%
    filter(sent_from_messaging_service == FALSE) %>%
    filter(!is.na(body)) %>%
    mutate(
      body = str_replace_all(body, "[^[:alnum:]]", " ") %>%
        str_squish() %>%
        str_to_lower(),
      is_unsubscribe = str_detect(body, paste(stop_keywords, collapse = "|")),
      is_resubscribe = str_detect(body, paste(start_keywords, collapse = "|"))
    ) %>%
    select(
      msg_id,
      customer = from, org_uuid,
      is_unsubscribe, is_resubscribe, customer_resp = body
    )

  # When was customer reached
  sms <- sms %>%
    mutate(customer = if_else(sent_from_messaging_service, to, from)) %>%
    left_join(from_customers, by = c("org_uuid", "customer", "msg_id")) %>%
    group_by(customer, org_uuid) %>%
    arrange(date_sent) %>%
    mutate_at(vars(is_unsubscribe, is_resubscribe), ~ coalesce(., FALSE)) %>%
    mutate(
      next_msg_is_unsubscribe = coalesce(lead(is_unsubscribe, 1, order_by = date_sent), FALSE),
      unsubscribe_date        = if_else(is_unsubscribe, date_sent, as_datetime(NA)),
      resubscribe_date        = if_else(is_resubscribe, date_sent, as_datetime(NA)),
      last_customer_resp      = customer_resp,
      last_msg_received       = if_else(sent_from_messaging_service, body, as.character(NA)),
      last_campaign_id        = if_else(sent_from_messaging_service, campaign_id, as.character(NA)),
      last_msg_recieved_date  = if_else(sent_from_messaging_service, date_sent, as_datetime(NA))
    ) %>%
    fill(
      unsubscribe_date, resubscribe_date, last_msg_received, last_campaign_id,
      last_msg_recieved_date, last_customer_resp
    ) %>%
    mutate(is_subscribed = !(
      coalesce(unsubscribe_date, as_datetime(0)) > coalesce(resubscribe_date, as_datetime(0))
    ))

  group_by(sms, phone = customer, org_uuid) %>%
    mutate(tot_sms_received = sum(sent_from_messaging_service, na.rm = TRUE)) %>%
    filter(row_number(date_sent) == max(row_number(date_sent))) %>%
    ungroup() %>%
    transmute(
      phone,
      org_uuid,
      is_subscribed,
      unsubscribed_at_utc     = format(with_tz(unsubscribe_date, "UTC"), "%Y-%m-%d %H:%M:%S"),
      resubscribed_at_utc     = format(with_tz(resubscribe_date, "UTC"), "%Y-%m-%d %H:%M:%S"),
      last_sms_engagement_utc = format(with_tz(last_msg_recieved_date, "UTC"), "%Y-%m-%d %H:%M:%S"),
      last_msg_received,
      last_customer_resp,
      last_campaign_id,
      tot_sms_received
    )
}
