# Marketing Studio App Data
# run.R
#
# (C) 2020 Happy Cabbage Analytics Inc.

# Source ------------------------------------------------------------------
org <- "hightimes"
source(
  stringr::str_glue("inst/pipeline/appdata/polaris/polaris_appdata/custom/{org}/init.R"),
  local = TRUE
)
source(
  stringr::str_glue(
    "inst/pipeline/appdata/polaris/polaris_appdata/custom/{org}/src/build_{org}_polaris.R"
  ),
  local = TRUE
)

# READ --------------------------------------------------------------------
pg <- do.call(DBI::dbConnect, hcaconfig::ld_odbc("prod2", "cabbage_patch"))
qry <-
  "SELECT
  tbl_outer.id as customer_id,
  loyalty_points,
  enable_loyalty,
  tags,
  shop_id,
  tbl_outer.run_date_utc
FROM {tbl} tbl_outer
INNER JOIN (
  SELECT
    id,
    max(run_date_utc) AS run_date_utc
  FROM {tbl}
  GROUP BY id
) tbl_inner
ON tbl_outer.run_date_utc = tbl_inner.run_date_utc
AND tbl_outer.id = tbl_inner.id"

outer_qry <- "
  SELECT * FROM ({stringr::str_glue(qry, tbl = 'hightimes_redding_blaze_members')}) redding
  UNION
  SELECT * FROM ({stringr::str_glue(qry, tbl = 'hightimes_shastalake_blaze_members')}) shasta
  UNION
  SELECT * FROM ({stringr::str_glue(qry, tbl = 'hightimes_oakland_blaze_members')}) oak
  UNION
  SELECT * FROM ({stringr::str_glue(qry, tbl = 'hightimes_coalinga_blaze_members')}) coalinga
  UNION
  SELECT * FROM ({stringr::str_glue(qry, tbl = 'hightimes_maywood_blaze_members')}) maywood
  UNION
  SELECT * FROM ({stringr::str_glue(qry, tbl = 'hightimes_sacramento_blaze_members')}) sac
  UNION
  SELECT * FROM ({stringr::str_glue(qry, tbl = 'hightimes_broadway_blaze_members')}) broadway
  UNION
  SELECT * FROM ({stringr::str_glue(qry, tbl = 'hightimes_blythe_blaze_members')}) blythe
  UNION
  SELECT * FROM ({stringr::str_glue(qry, tbl = 'hightimes_sanbernardino_blaze_members')})
    sanbernardino
"
shopQry <- "
  (SELECT shop_id, 'BROADWAY' AS shop FROM hightimes_broadway_blaze_transactions LIMIT 1)
  UNION
  (SELECT shop_id, 'COALINGA' AS shop FROM hightimes_coalinga_blaze_transactions LIMIT 1)
  UNION
  (SELECT shop_id, 'MAYWOOD' AS shop FROM hightimes_maywood_blaze_transactions LIMIT 1)
  UNION
  (SELECT shop_id, 'OAKLAND' AS shop FROM hightimes_oakland_blaze_transactions LIMIT 1)
  UNION
  (SELECT shop_id, 'SACRAMENTO' AS shop FROM hightimes_sacramento_blaze_transactions LIMIT 1)
  UNION
  (SELECT shop_id, 'SHASTA LAKE' AS shop FROM hightimes_shastalake_blaze_transactions LIMIT 1)
  UNION
  (SELECT shop_id, 'BLYTHE' AS shop FROM hightimes_blythe_blaze_transactions LIMIT 1)
  UNION
  (SELECT shop_id, 'REDDING' AS shop FROM hightimes_redding_blaze_transactions LIMIT 1)
  UNION
  (SELECT shop_id, 'SAN BERNARDINO' AS shop FROM hightimes_sanbernardino_blaze_transactions LIMIT 1)
"
raw <- DBI::dbGetQuery(pg, stringr::str_glue(
  "SELECT members.*, shops.shop
   FROM ({stringr::str_glue(outer_qry)}) members
   LEFT JOIN ({shopQry}) shops
   ON members.shop_id = shops.shop_id"
))
DBI::dbDisconnect(pg)


# Integrated
cn <- do.call(DBI::dbConnect, hcaconfig::ld_odbc("prod2", "integrated"))
customer_behavior <- DBI::dbGetQuery(
  cn,
  stringr::str_glue("SELECT * FROM customer_behavior WHERE org IN ('{org}') ")
)
customers <- DBI::dbGetQuery(
  cn,
  stringr::str_glue("SELECT * FROM customers WHERE org IN ('{org}') ")
)
DBI::dbDisconnect(cn)

# Run ---------------------------------------------------------------
polaris <- build_hightimes_polaris(customers, customer_behavior)
polaris$run_date_utc <- lubridate::now(tzone = "UTC")

loyalty <- build_ht_loyalty(raw)
polaris <- polaris %>%
  dplyr::left_join(loyalty) %>%
  dplyr::mutate(
    tags = dplyr::coalesce(tags, "No Group"),
    last_order_facility = dplyr::if_else(
      num_orders == 0 & !is.na(shop),
      paste0(last_order_facility, " - ", shop),
      last_order_facility
    )
  ) %>%
  dplyr::select(-shop, -shop_id, -orguuid)

# Write ---------------------------------------------------------------
orgQuery <- paste(unique(polaris$org), collapse = "', '")
pg <- hcaconfig::dbc("prod2", "appdata")
DBI::dbBegin(pg)
DBI::dbExecute(pg, stringr::str_glue(" DELETE FROM polaris_appdata WHERE org IN ('{orgQuery}') "))
DBI::dbWriteTable(pg, "polaris_appdata", value = polaris, append = TRUE)
DBI::dbCommit(pg)
hcaconfig::dbd(pg)
