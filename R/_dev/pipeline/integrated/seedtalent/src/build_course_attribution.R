box::use(
  dplyr[
    arrange, bind_rows, coalesce, distinct, filter, group_by, mutate, row_number, select, transmute,
    ungroup
  ],
  lubridate[ymd_hms],
  tidyr[fill]
)

build_course_attribution <- function(orders_df, test_results_df) {
  # Extract order timestamp info
  order_info <- orders_df |>
    mutate(
      posId = sold_by_id,
      order_time_utc = ymd_hms(order_time_utc, tz = "UTC"),
      event_time_utc = order_time_utc,
      event = "order"
    ) |>
    distinct()
  # Extract course completion information
  completions <- test_results_df |>
    transmute(
      posId,
      course_id,
      course_completed_utc = ymd_hms(end, tz = "UTC"),
      event_time_utc = course_completed_utc,
      event = "training"
    ) |>
    distinct() |>
    group_by(posId) |>
    mutate(n_course = row_number(course_completed_utc)) |>
    ungroup()
  # STACK
  stacked <- bind_rows(order_info, completions)
  # Find last touch
  last_touch_attributed <- stacked |>
    mutate(
      last_course_id = course_id,
      last_course_completed_utc = course_completed_utc,
      last_n_course = n_course,
      next_course_id = course_id,
      next_course_completed_utc = course_completed_utc,
      next_n_course = n_course
    ) |>
    group_by(posId) |>
    arrange(event_time_utc) |>
    fill(last_course_id, last_course_completed_utc, last_n_course) |>
    fill(next_course_id, next_course_completed_utc, next_n_course, .direction = "up") |>
    mutate(has_been_trained = suppressWarnings(!is.infinite(max(n_course, na.rm = TRUE)))) |>
    ungroup() |>
    select(-course_id, -course_completed_utc, -n_course) |>
    filter(event == "order", has_been_trained == TRUE) |>
    mutate(
      last_n_course = coalesce(last_n_course, 0),
      time_to_next_course = as.numeric(difftime(
        next_course_completed_utc, event_time_utc,
        units = "days"
      )),
      time_since_last_course = as.numeric(difftime(
        event_time_utc, last_course_completed_utc,
        units = "days"
      )),
      time_normalized = coalesce(time_to_next_course * -1, time_since_last_course),
      course_ts_utc = coalesce(last_course_completed_utc, next_course_completed_utc)
    )
  return(last_touch_attributed)
}
