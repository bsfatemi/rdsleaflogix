# Seed Talent API PL
# build_training_targets.R
# (C) Happy Cababge Analytics, Inc. 2022

box::use(
  dplyr[case_when, filter, group_by, if_else, mutate, rename, select, ungroup],
  stats[sd]
)

build_training_targets <- function(df, kpi, sensitivity = 1, direction = "under") {
  if (!kpi %in% c("ticket_size", "pct_disc", "pct_upsold")) {
    stop("Invalid KPI selected - must be one of: 'ticket_size', 'pct_disc', or 'pct_upsold'")
  }
  if (!direction %in% c("under", "over")) {
    stop("Invalid direction parameter - must be one of 'under' or 'over'")
  }
  chng <- paste0(kpi, "_chng")
  targets <- df |>
    rename(
      current_value = {{ kpi }},
      change = {{ chng }}
    ) |>
    group_by(store) |>
    mutate(
      kpi = kpi,
      distance = current_value / mean(current_value) - 1,
      mean = mean(current_value),
      threshold = mean(current_value) + if_else(
        direction == "under",
        sd(current_value) * sensitivity * -1,
        sd(current_value) * sensitivity
      ),
      rev_opportunity = case_when(
        kpi == "ticket_size" ~ (mean(current_value) - current_value) * (orders - preorders),
        kpi == "pct_upsold" ~ (mean(current_value) - current_value) * (preorders) * mean_upsell,
        kpi == "pct_disc" ~ (pretax_sales / 1 + mean(current_value)) - pretax_sales
      )
    ) |>
    select(
      budtender = sold_by,
      pos_id = sold_by_id,
      store,
      store_uuid,
      kpi,
      current_value,
      change,
      distance,
      rev_opportunity,
      threshold,
      mean
    ) |>
    ungroup()
  if (direction == "under") {
    targets <- filter(targets, current_value < threshold)
  } else {
    targets <- filter(targets, current_value > threshold)
  }
  return(targets)
}
