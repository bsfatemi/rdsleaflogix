# Seed Talent API PL
# build_ticket_roi.R
# (C) Happy Cababge Analytics, Inc. 2022

box::use(
  dplyr[group_by, if_else, n_distinct, summarize, transmute],
  lubridate[date, today]
)

# Aux function.
day_orders <- function(time_to_course, threshold, order_id) {
  n_distinct(if_else(time_to_course <= threshold, order_id, NA_character_), na.rm = TRUE)
}
# Aux function.
mean_day_subtotals <- function(time_to_course, threshold, order_subtotal) {
  mean(if_else(time_to_course <= threshold, order_subtotal, NA_real_), na.rm = TRUE)
}

build_ticket_roi <- function(ticket_size_attribution, org, CID) {
  ticket_roi <- ticket_size_attribution |>
    group_by(sold_by = toupper(sold_by), posId, course_ts_utc, store, store_uuid) |>
    summarize(
      post_7day_orders = day_orders(time_since_last_course, 7, order_id),
      post_14day_orders = day_orders(time_since_last_course, 14, order_id),
      post_30day_orders = day_orders(time_since_last_course, 30, order_id),
      post_60day_orders = day_orders(time_since_last_course, 60, order_id),
      post_90day_orders = day_orders(time_since_last_course, 90, order_id),
      post_7day_mean = mean_day_subtotals(time_since_last_course, 7, order_subtotal),
      post_14day_mean = mean_day_subtotals(time_since_last_course, 14, order_subtotal),
      post_30day_mean = mean_day_subtotals(time_since_last_course, 30, order_subtotal),
      post_60day_mean = mean_day_subtotals(time_since_last_course, 60, order_subtotal),
      post_90day_mean = mean_day_subtotals(time_since_last_course, 90, order_subtotal),
      pre_7day_mean = mean_day_subtotals(time_to_next_course, 7, order_subtotal),
      pre_14day_mean = mean_day_subtotals(time_to_next_course, 14, order_subtotal),
      pre_30day_mean = mean_day_subtotals(time_to_next_course, 30, order_subtotal),
      pre_60day_mean = mean_day_subtotals(time_to_next_course, 60, order_subtotal),
      pre_90day_mean = mean_day_subtotals(time_to_next_course, 90, order_subtotal),
      .groups = "drop"
    )

  ticket_roi_summary <- ticket_roi |>
    transmute(
      org_uuid = org$org_uuid,
      course_id = CID,
      store,
      store_uuid,
      sold_by,
      pos_id = posId,
      course_ts_utc,
      days_since_course = as.numeric(difftime(
        today(tzone = "UTC"), date(course_ts_utc), units = "days"
      )),
      kpi = "ticket_size",
      pre_7day = pre_7day_mean,
      post_7day = post_7day_mean,
      uplift_7day = (post_7day_mean - pre_30day_mean) / pre_30day_mean,
      dollars_added_7day = (post_7day_mean - pre_30day_mean) * post_7day_orders,
      pre_14day = pre_14day_mean,
      post_14day = post_14day_mean,
      uplift_14day = (post_14day_mean - pre_30day_mean) / pre_30day_mean,
      dollars_added_14day = (post_14day_mean - pre_30day_mean) * post_14day_orders,
      pre_30day = pre_30day_mean,
      post_30day = post_30day_mean,
      uplift_30day = (post_30day_mean - pre_30day_mean) / pre_30day_mean,
      dollars_added_30day = (post_30day_mean - pre_30day_mean) * post_30day_orders,
      pre_60day = pre_60day_mean,
      post_60day = post_60day_mean,
      uplift_60day = (post_60day_mean - pre_30day_mean) / pre_30day_mean,
      dollars_added_60day = (post_60day_mean - pre_30day_mean) * post_60day_orders,
      pre_90day = pre_90day_mean,
      post_90day = post_90day_mean,
      uplift_90day = (post_90day_mean - pre_30day_mean) / pre_30day_mean,
      dollars_added_90day = (post_90day_mean - pre_30day_mean) * post_90day_orders,
    )
}


# Aux function.
day_orders_upsells <- function(time_to_course, threshold, is_upsell, order_id) {
  n_distinct(
    if_else(time_to_course <= threshold & is_upsell, order_id, NA_character_), na.rm = TRUE
  )
}
# Aux function.
day_avgupsell <- function(time_to_course, threshold, is_upsell, preorder_change_dollars,
                          day_upsells) {
  sum((time_to_course <= threshold & is_upsell) * preorder_change_dollars, na.rm = TRUE) /
    day_upsells
}

build_bopis_roi <- function(bopis_attribution, org, CID) {
  ## EXPLORE SOME DATA REALLY QUICKLY
  bopis_roi <- bopis_attribution |>
    group_by(sold_by = toupper(sold_by), posId, course_ts_utc, store, store_uuid) |>
    summarize(
      post_7day_orders = day_orders(time_since_last_course, 7, order_id),
      post_14day_orders = day_orders(time_since_last_course, 14, order_id),
      post_30day_orders = day_orders(time_since_last_course, 30, order_id),
      post_60day_orders = day_orders(time_since_last_course, 60, order_id),
      post_90day_orders = day_orders(time_since_last_course, 90, order_id),
      pre_7day_orders = day_orders(time_to_next_course, 7, order_id),
      pre_14day_orders = day_orders(time_to_next_course, 14, order_id),
      pre_30day_orders = day_orders(time_to_next_course, 30, order_id),
      pre_60day_orders = day_orders(time_to_next_course, 60, order_id),
      pre_90day_orders = day_orders(time_to_next_course, 90, order_id),
      post_7day_upsells = day_orders_upsells(time_since_last_course, 7, is_upsell, order_id),
      post_14day_upsells = day_orders_upsells(time_since_last_course, 14, is_upsell, order_id),
      post_30day_upsells = day_orders_upsells(time_since_last_course, 30, is_upsell, order_id),
      post_60day_upsells = day_orders_upsells(time_since_last_course, 60, is_upsell, order_id),
      post_90day_upsells = day_orders_upsells(time_since_last_course, 90, is_upsell, order_id),
      pre_7day_upsells = day_orders_upsells(time_to_next_course, 7, is_upsell, order_id),
      pre_14day_upsells = day_orders_upsells(time_to_next_course, 14, is_upsell, order_id),
      pre_30day_upsells = day_orders_upsells(time_to_next_course, 30, is_upsell, order_id),
      pre_60day_upsells = day_orders_upsells(time_to_next_course, 60, is_upsell, order_id),
      pre_90day_upsells = day_orders_upsells(time_to_next_course, 90, is_upsell, order_id),
      post_7day_upsold = post_7day_upsells / post_7day_orders,
      post_14day_upsold = post_14day_upsells / post_14day_orders,
      post_30day_upsold = post_30day_upsells / post_30day_orders,
      post_60day_upsold = post_60day_upsells / post_60day_orders,
      post_90day_upsold = post_90day_upsells / post_90day_orders,
      pre_7day_upsold = pre_7day_upsells / pre_7day_orders,
      pre_14day_upsold = pre_14day_upsells / pre_14day_orders,
      pre_30day_upsold = pre_30day_upsells / pre_30day_orders,
      pre_60day_upsold = pre_60day_upsells / pre_60day_orders,
      pre_90day_upsold = pre_90day_upsells / pre_90day_orders,
      post_7day_avgupsell = day_avgupsell(
        time_since_last_course, 7, is_upsell, preorder_change_dollars, post_7day_upsells
      ),
      post_14day_avgupsell = day_avgupsell(
        time_since_last_course, 14, is_upsell, preorder_change_dollars, post_14day_upsells
      ),
      post_30day_avgupsell = day_avgupsell(
        time_since_last_course, 30, is_upsell, preorder_change_dollars, post_30day_upsells
      ),
      post_60day_avgupsell = day_avgupsell(
        time_since_last_course, 60, is_upsell, preorder_change_dollars, post_60day_upsells
      ),
      post_90day_avgupsell = day_avgupsell(
        time_since_last_course, 90, is_upsell, preorder_change_dollars, post_90day_upsells
      ),
      .groups = "drop"
    )

  bopis_roi_summary <- bopis_roi |>
    transmute(
      org_uuid = org$org_uuid,
      store,
      store_uuid,
      course_id = CID,
      sold_by,
      pos_id = posId,
      course_ts_utc,
      days_since_course = as.numeric(difftime(
        today(tzone = "UTC"), date(course_ts_utc), units = "days"
      )),
      kpi = "pct_upsold",
      pre_7day = pre_7day_upsold,
      post_7day = post_7day_upsold,
      uplift_7day = (post_7day_upsold - pre_30day_upsold) / pre_30day_upsold,
      dollars_added_7day = (post_7day_upsold - pre_30day_upsold) * post_7day_orders *
        post_7day_avgupsell,
      pre_14day = pre_14day_upsold,
      post_14day = post_14day_upsold,
      uplift_14day = (post_14day_upsold - pre_30day_upsold) / pre_30day_upsold,
      dollars_added_14day = (post_14day_upsold - pre_30day_upsold) * post_14day_orders *
        post_14day_avgupsell,
      pre_30day = pre_30day_upsold,
      post_30day = post_30day_upsold,
      uplift_30day = (post_30day_upsold - pre_30day_upsold) / pre_30day_upsold,
      dollars_added_30day = (post_30day_upsold - pre_30day_upsold) * post_30day_orders *
        post_30day_avgupsell,
      pre_60day = pre_60day_upsold,
      post_60day = post_60day_upsold,
      uplift_60day = (post_60day_upsold - pre_30day_upsold) / pre_30day_upsold,
      dollars_added_60day = (post_60day_upsold - pre_30day_upsold) * post_60day_orders *
        post_60day_avgupsell,
      pre_90day = pre_90day_upsold,
      post_90day = post_90day_upsold,
      uplift_90day = (post_90day_upsold - pre_30day_upsold) / pre_30day_upsold,
      dollars_added_90day = (post_90day_upsold - pre_30day_upsold) * post_90day_orders *
        post_90day_avgupsell,
    )
}
