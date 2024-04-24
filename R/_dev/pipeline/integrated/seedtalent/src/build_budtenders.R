# Seed Talent API PL
# build_budtenders.R
# (C) Happy Cababge Analytics, Inc. 2022

box::use(
  dplyr[
    arrange, distinct, filter, group_by, if_else, inner_join, lag, mutate, n_distinct, summarize,
    ungroup
  ],
  lubridate[days, date, hour, today],
  stats[median]
)

build_budtenders <- function(pop, t0_days = 180, t1_days = 30) {
  ## TODO: EXTRA DATA ON ORDER SOURCE AND PREORDER AMOUNTS NEEDS TO BE MOVED INTO
  #        COLUMNS IN POPULATION
  # VALUES
  # SUMMARIZE INTO BUDTENDER LEVEL DATA
  recent_bt <- pop |>
    filter(date(order_time_local) > today() - days(t1_days)) |>
    distinct(sold_by_id)
  sample <- pop |>
    filter(
      order_dt > today() - days(t0_days),
      !is.na(sold_by),
      order_discount <= 0,
      order_subtotal >= 0,
      !is.na(customer_id),
      order_type == "Retail"
    ) |>
    inner_join(recent_bt, by = "sold_by_id")
  ## CREATE A DATAFRAME WITH BUDTENDERS WHO HAVE PURCHASE RECENTLY,
  #  A SET OF KPIS ATTACHED TO THEM
  #  AND SOME SUMMARY STATISTICS
  kpis <- sample |>
    group_by(sold_by = toupper(sold_by), sold_by_id, in_eval_period, store, store_uuid) |>
    summarize(
      first_dt = min(order_dt),
      last_dt = max(order_dt),
      tenure_days = n_distinct(order_dt),
      orders = n_distinct(order_id),
      pretax_sales = sum(order_subtotal, na.rm = TRUE),
      orders_per_day = orders / tenure_days,
      preorders = n_distinct(
        if_else(was_pickup == TRUE, order_id, NA_character_),
        na.rm = TRUE
      ),
      preorders_per_day = preorders / tenure_days,
      earliest_hr = min(hour(order_time_local)),
      latest_hr = max(hour(order_time_local)),
      busiest_hr = median(hour(order_time_local)),

      ## KPIS TO BE TARGETED ON
      ticket_size = mean(
        if_else(channel == "WALK-IN", order_subtotal, NA_real_),
        na.rm = TRUE
      ),
      pct_disc = mean(
        if_else(channel == "WALK-IN", order_discount / (order_subtotal - order_discount), NA_real_),
        na.rm = TRUE
      ),
      # ticket_time         = median(
      #   if_else(channel == "IN-STORE", as.numeric(checkin_to_trans), NA_real_), na.rm = TRUE
      # ),
      pct_upsold = sum(channel == "PREORDER" & is_upsell, na.rm = TRUE) / n_distinct(
        if_else(channel == "PREORDER", order_id, NA_character_),
        na.rm = TRUE
      ),
      mean_upsell = sum(
        if_else(is_upsell & channel == "PREORDER", preorder_change_dollars, 0),
        na.rm = TRUE
      ) / sum(channel == "PREORDER" & is_upsell, na.rm = TRUE),
      .groups = "drop"
    ) |>
    group_by(sold_by, sold_by_id, store) |>
    arrange(in_eval_period) |>
    mutate(
      starting_vol = lag(orders, 1),
      starting_preorder_vol = lag(preorders, 1),
      ticket_size_chng = ticket_size / lag(ticket_size, 1) - 1,
      pct_disc_chng = pct_disc / lag(pct_disc, 1) - 1,
      # ticket_time_chng = ticket_time / lag(ticket_time, 1) -1,
      pct_upsold_chng = pct_upsold / lag(pct_upsold, 1) - 1,
      mean_uspell_chng = mean_upsell / lag(mean_upsell, 1) - 1
    ) |>
    ungroup()
  filter(kpis, in_eval_period == TRUE)
}
