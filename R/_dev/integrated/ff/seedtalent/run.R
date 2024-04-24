# Purge the box module cache, so the app can be reloaded without restarting the R session.
rm(list = ls(box:::loaded_mods), envir = box:::loaded_mods)

# Allow absolute module imports (relative to the project root).
options(box.path = getwd())

box::use(
  DBI[dbAppendTable, dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[
    bind_rows, coalesce, filter, group_by, inner_join, left_join, mutate, rename, select, ungroup
  ],
  glue[glue],
  hcaconfig[dbc, dbd, orgIndex],
  hcaseedtalent[get_api_keys],
  jsonlite[fromJSON],
  lubridate[days, now, today],
  purrr[map, transpose, walk]
)

box::use(
  inst/pipeline/integrated/seedtalent/src/build_budtenders[build_budtenders],
  inst/pipeline/integrated/seedtalent/src/build_course_attribution[build_course_attribution],
  inst/pipeline/integrated/seedtalent/src/build_jane_customer_preorders[
    build_jane_customer_preorders
  ],
  inst/pipeline/integrated/seedtalent/src/build_pop_info[build_pop_info],
  inst/pipeline/integrated/seedtalent/src/build_roi[build_bopis_roi, build_ticket_roi],
  inst/pipeline/integrated/seedtalent/src/build_training_targets[build_training_targets],
  inst/pipeline/integrated/seedtalent/src/read_jane[read_jane],
  inst/pipeline/integrated/seedtalent/src/seedtalent[
    build_seedtalent_courses, build_seedtalent_tests
  ]
)

orgs <- get_api_keys() |>
  inner_join(orgIndex()[curr_client & in_pop, c("org_uuid", "tz")], by = "org_uuid")
as_of_date <- today() - days(365)
(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))

pg <- dbc(db_cfg, "hcaconfig")
seedtalent_conf <- dbReadTable(pg, "seedtalent_conf")
excluded_budtenders <- dbReadTable(pg, "seedtalent_excluded_budtenders")
dbd(pg)

walk(transpose(orgs), function(org) {
  message(org$short_name)

  # POPULATION
  message("Reading population...")
  org_seedtalent_conf <- filter(seedtalent_conf, org_uuid == org$org_uuid)
  pg <- dbc(db_cfg, "integrated")
  # Write to temporary DB the org's excluded budtenders, to antijoin from population.
  org_excluded_budtenders <- filter(excluded_budtenders, org_uuid == org$org_uuid) |>
    rename(store_id = store_uuid)
  excluded_bts_tbl_all <- paste0(org$short_name, "_excluded_budtenders")
  excluded_bts_tbl_stores <- paste0(org$short_name, "_excluded_budtenders_stores")
  dbWriteTable(
    pg, excluded_bts_tbl_all, filter(org_excluded_budtenders, is.na(store_id)), temporary = TRUE
  )
  dbWriteTable(
    pg, excluded_bts_tbl_stores, filter(org_excluded_budtenders, !is.na(store_id)), temporary = TRUE
  )
  # Get population, with the budtender antijoins.
  org_uuids <- org$org_uuid
  if (!is.na(org_seedtalent_conf$additional_org_uuids)) {
    org_uuids <- c(org_uuids, fromJSON(org_seedtalent_conf$additional_org_uuids))
  }
  pop <- dbGetQuery(pg, glue("
    SELECT
      order_id, order_source, order_type, facility, order_time_local, order_time_utc, sold_by,
      sold_by_id, customer_id, user_type, phone, order_line_id, product_id, brand_name,
      raw_category_name, order_total, order_discount, order_subtotal, order_tax,
      order_line_discount, store_id
    FROM population
    WHERE
      org_uuid IN ('{paste0(org_uuids, collapse = \"','\")}') AND
      date(order_time_local) > '{as_of_date}' AND
      (sold_by_id) NOT IN (SELECT sold_by_id FROM {excluded_bts_tbl_all}) AND
      (store_id, sold_by_id) NOT IN (SELECT store_id, sold_by_id FROM {excluded_bts_tbl_stores});
  "))
  dbd(pg)

  # Fix in case there is no `order_discount`.
  pop <- group_by(pop, order_id) |>
    mutate(order_discount_sum = sum(order_line_discount, na.rm = TRUE)) |>
    ungroup() |>
    mutate(order_discount = coalesce(order_discount, order_discount_sum)) |>
    select(-order_discount_sum, -order_line_discount)

  # JANE
  message("Building budtenders performance...")
  preorders <- NULL
  jane_stores <- fromJSON(org_seedtalent_conf$stores) |>
    lapply(function(x) x$ihjane_names)
  if (length(unlist(jane_stores)) > 0) {
    pg <- dbc(db_cfg, "cabbage_patch")
    jane <- read_jane(pg, org$short_name, jane_stores)
    dbd(pg)
    preorders <- build_jane_customer_preorders(jane, tz = org$tz)
  }

  # Staff Statistics
  now_utc <- now("UTC")
  pop_order_info <- build_pop_info(pop, preorders, t1_days = 30)
  budtenders_performance <- build_budtenders(pop_order_info, t0_days = 180, t1_days = 30) |>
    mutate(org_uuid = org$org_uuid, run_date_utc = now_utc)

  # Push budtenders performance data.
  pg <- dbc(db_cfg, "integrated")
  dbBegin(pg)
  dbExecute(pg, paste0(
    "DELETE FROM seedtalent_budtenders_performance WHERE org_uuid = '", org$org_uuid, "'"
  ))
  dbAppendTable(pg, "seedtalent_budtenders_performance", budtenders_performance)
  dbCommit(pg)
  dbd(pg)

  message("Building budtenders targets...")
  courses <- list(
    # TICKET SIZE
    "1b9cc904-e6bc-47b6-ad04-65a18546167b" = list(
      kpi = "ticket_size",
      sensitivity = 1,
      direction = "under",
      df = filter(
        budtenders_performance,
        starting_vol > 20,
        orders - preorders > 50,
        ticket_size_chng < 1,
        ticket_size_chng > -1
      )
    ),
    # BOPIS
    "cb3b994f-115c-4441-9d54-a6ca2a8f4a80" = list(
      kpi = "pct_upsold",
      sensitivity = 1,
      direction = "under",
      df = filter(
        budtenders_performance,
        starting_vol > 20,
        preorders > 50
      )
    )
  )

  # AGG
  targets <- courses |>
    map(~ do.call("build_training_targets", args = .x)) |>
    bind_rows(.id = "course_id") |>
    mutate(org_uuid = org$org_uuid, run_date_utc = now_utc)

  # Push budtenders targets data.
  pg <- dbc(db_cfg, "integrated")
  dbBegin(pg)
  dbExecute(pg, paste0(
    "DELETE FROM seedtalent_budtenders_targets WHERE org_uuid = '", org$org_uuid, "'"
  ))
  dbAppendTable(pg, "seedtalent_budtenders_targets", targets)
  dbCommit(pg)
  dbd(pg)

  # ROI
  message("Building budtenders ROI...")
  pg <- dbc(db_cfg, "cabbage_patch")
  courses <- build_seedtalent_courses(pg)
  res_df <- build_seedtalent_tests(pg, org$short_name)
  dbd(pg)
  st_df <- left_join(res_df, courses, by = "course_id")

  CID <- "1b9cc904-e6bc-47b6-ad04-65a18546167b"
  ticket_size_attribution <- build_course_attribution(
    orders_df = filter(pop_order_info, was_pickup == FALSE),
    test_results_df = filter(st_df, course_id == CID)
  )
  ticket_roi_summary <- build_ticket_roi(ticket_size_attribution, org, CID)

  CID <- "cb3b994f-115c-4441-9d54-a6ca2a8f4a80"
  bopis_attribution <- build_course_attribution(
    orders_df = filter(pop_order_info, was_pickup == TRUE),
    test_results_df = filter(st_df, course_id == CID)
  )
  bopis_roi_summary <- build_bopis_roi(bopis_attribution, org, CID)

  # Push budtenders course results data.
  course_results <- bind_rows(ticket_roi_summary, bopis_roi_summary) |>
    mutate(run_date_utc = now_utc)
  pg <- dbc(db_cfg, "integrated")
  dbBegin(pg)
  dbExecute(pg, paste0(
    "DELETE FROM seedtalent_budtenders_course_results WHERE org_uuid = '", org$org_uuid, "'"
  ))
  dbAppendTable(pg, "seedtalent_budtenders_course_results", course_results)
  dbCommit(pg)
  dbd(pg)
})
