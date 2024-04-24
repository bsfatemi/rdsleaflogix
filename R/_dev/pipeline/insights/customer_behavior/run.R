# Consolidated Customers
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

# Source ------------------------------------------------------------------
source("inst/pipeline/insights/customer_behavior/init.R", local = TRUE)
source("inst/pipeline/insights/customer_behavior/src/build_customer_behavior.R", local = TRUE)

box::use(
  DBI[dbBegin, dbCommit, dbGetQuery, dbExecute, dbWriteTable],
  dbplyr[...], # Used as dplyr SQL backend.
  dplyr[collect, distinct, group_by, if_else, left_join, select, summarise, tbl],
  dtplyr[lazy_dt],
  glue[glue],
  hcaconfig[dbc, dbd, orgIndex],
  hcapipelines[plAppDataIndex],
  jsonlite[fromJSON, toJSON],
  parallel[mclapply]
)

orgs <- as.data.frame(
  plAppDataIndex()[
    in_population == TRUE & polaris_is_active == TRUE & current_client == TRUE & pos_data == TRUE
  ]
)

# Import ------------------------------------------------------------------
behavior <- mclapply(orgs$short_name, function(org) {
  try({
    pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "integrated")
    population <- dbGetQuery(pg, glue("
      SELECT
        org,
        customer_id,
        source_system,
        order_time_local,
        order_id,
        order_line_id,
        order_time_utc,
        order_discount,
        order_total,
        order_type,
        order_facility,
        product_class,
        order_source,
        COALESCE(brand_name, SUBSTRING(product_name, 1, 100)) AS brand,
        CASE
          WHEN category3 = 'PREROLLS' THEN 'preroll'
          WHEN category3 = 'EDIBLES'  THEN 'edible'
          WHEN category3 = 'EXTRACTS' THEN 'extract'
          WHEN category3 = 'TOPICALS' THEN 'topical'
          ELSE LOWER(category3)
        END AS category3
      FROM population
      WHERE customer_id IS NOT NULL AND org = '{org}'
    "))
    # Calculate SMS conversion data.
    sms_conversion <- tbl(pg, "sms_attributed_order_lines") |>
      # Distinct by order (not by order_line).
      distinct(
        org, customer_id, last_sms_campaign_id, time_since_last_order, order_id, is_24hr, is_72hr,
        is_7day, is_2wk, is_30day
      ) |>
      # Need to check for the most recent purchase per campaign Id and only count those statistics
      group_by(org, customer_id, last_sms_campaign_id) |>
      filter(time_since_last_order == min(time_since_last_order) & org == !!org) |>
      group_by(org, customer_id) |>
      # Per customer, sum how many attributed orders in each of the time windows.
      summarise(
        tot_24hr_conversion = sum(if_else(is_24hr, 1, 0), na.rm = TRUE),
        tot_72hr_conversion = sum(if_else(is_72hr, 1, 0), na.rm = TRUE),
        tot_7day_conversion = sum(if_else(is_7day, 1, 0), na.rm = TRUE),
        tot_2wk_conversion = sum(if_else(is_2wk, 1, 0), na.rm = TRUE),
        tot_30day_conversion = sum(if_else(is_30day, 1, 0), na.rm = TRUE),
        .groups = "drop"
      ) |>
      collect()
    dbd(pg)
    # BUILD -------------------------------------------------------------------

    behavior <- build_order_behavior(population) %>%
      left_join(build_product_behavior(population), by = c("org", "customer_id")) %>%
      left_join(
        build_customer_prefs(
          # We want to filter out these categories, if done here, percentages will sum 1 for every
          # customer.
          filter(population, !category3 %in% c("accessories", "other")), col = "category3"
        ),
        by = c("org", "customer_id")
      ) %>%
      left_join(
        build_customer_prefs(population, col = "product_class"), by = c("org", "customer_id")
      ) %>%
      left_join(sms_conversion, by = c("org", "customer_id")) |>
      distinct()
    if (length(names(behavior)) < 26) {
      stop("Customer Behavior Columns Not Correct")
    }
    # Write -------------------------------------------------------------------
    pg_out <- dbc(Sys.getenv("HCA_ENV", "prod2"), "integrated")
    dbBegin(pg_out)
    dbExecute(pg_out, glue("DELETE FROM customer_behavior WHERE org = '{org}' "))
    # Write the org's population to database.
    dbWriteTable(pg_out, "customer_behavior", behavior, append = TRUE)
    dbCommit(pg_out)
    dbd(pg_out)
  })
}, mc.cores = 4L)


# Database maintenance for population -----------------------------------------
pg_out <- dbc(Sys.getenv("HCA_ENV", "prod2"), "integrated")
temp_table_name <- "customer_behavior_temp"
dbWriteTable(pg_out, temp_table_name, distinct(orgs, short_name), temporary = TRUE)
dbBegin(pg_out)
dbExecute(pg_out, paste0(
  'DELETE FROM customer_behavior WHERE org NOT IN (SELECT short_name FROM "',
  temp_table_name, '")'
))
dbCommit(pg_out)
dbExecute(pg_out, "VACUUM ANALYZE customer_behavior")
dbd(pg_out)

errored_orgs <- orgs$short_name[
  sapply(behavior, function(x) inherits(x, "try-error"))
]
if (length(errored_orgs) > 0) {
  stop("Population build failed for: ", paste(errored_orgs, collapse = ", "))
}
