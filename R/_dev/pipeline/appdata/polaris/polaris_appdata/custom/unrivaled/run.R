# Marketing Studio App Data
# run.R
#
# (C) 2020 Happy Cabbage Analytics Inc.

# Source ------------------------------------------------------------------
org <- "unrivaled"
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
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
qry <-
  "SELECT
  tbl_outer.id as customer_id,
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
  SELECT * FROM ({stringr::str_glue(qry, tbl = 'unrivaled_blum_oakland_blaze_members')}) oakland
  UNION
  SELECT * FROM ({stringr::str_glue(qry, tbl = 'unrivaled_peoples_dtla_blaze_members')}) dtla
  UNION
  SELECT * FROM ({stringr::str_glue(qry, tbl = 'unrivaled_peoples_oc_blaze_members')}) oc
  UNION
  SELECT * FROM ({stringr::str_glue(qry, tbl = 'unrivaled_sacramento_blaze_members')}) sac
  UNION
  SELECT * FROM ({stringr::str_glue(qry, tbl = 'unrivaled_silverstreak_sanleandro_blaze_members')})
    ss
  UNION
  SELECT * FROM ({stringr::str_glue(qry, tbl = 'unrivaled_thespot_oc_blaze_members')}) thespot
"
shopQry <- "
  (SELECT shop_id, 'Blum Oakland' AS shop FROM unrivaled_blum_oakland_blaze_transactions LIMIT 1)
  UNION
  (SELECT shop_id, 'Peoples DTLA' AS shop FROM unrivaled_peoples_dtla_blaze_transactions LIMIT 1)
  UNION
  (SELECT shop_id, 'Peoples OC' AS shop FROM unrivaled_peoples_oc_blaze_transactions LIMIT 1)
  UNION
  (SELECT shop_id, 'Silverstreak Sacramento' AS shop FROM
    unrivaled_sacramento_blaze_transactions LIMIT 1)
  UNION
  (SELECT shop_id, 'Silverstreak San Leandro' AS shop FROM
    unrivaled_silverstreak_sanleandro_blaze_transactions LIMIT 1)
  UNION
  (SELECT shop_id, 'The Spot OC' AS shop FROM unrivaled_thespot_oc_blaze_transactions LIMIT 1)
"
raw <- DBI::dbGetQuery(pg, stringr::str_glue(
  "SELECT members.*, shops.shop
   FROM ({stringr::str_glue(outer_qry)}) members
   LEFT JOIN ({shopQry}) shops
   ON members.shop_id = shops.shop_id"
))
hcaconfig::dbd(pg)


# Integrated
cn <- hcaconfig::dbc("prod2", "integrated")
customer_behavior <- DBI::dbGetQuery(
  cn,
  stringr::str_glue("SELECT * FROM customer_behavior WHERE org IN ('{org}') ")
)
customers <- DBI::dbGetQuery(
  cn,
  stringr::str_glue("SELECT * FROM customers WHERE org IN ('{org}') ")
)
optin_status <- DBI::dbGetQuery(cn, stringr::str_glue(
  "SELECT * FROM optin_status WHERE orguuid IN ('{hcaconfig::lookupOrgGuid(org)}')"
))
hcaconfig::dbd(cn)

# Run ---------------------------------------------------------------
polaris <- build_unrivaled_polaris(customers, customer_behavior, optin_status)
polaris$run_date_utc <- lubridate::now(tzone = "UTC")

dedup <- raw %>%
  dplyr::group_by(customer_id) %>%
  dplyr::filter(dplyr::row_number(run_date_utc) == max(dplyr::row_number(run_date_utc))) %>%
  dplyr::ungroup() %>%
  dplyr::select(-run_date_utc)

polaris <- polaris %>%
  dplyr::left_join(dedup) %>%
  dplyr::mutate(
    last_order_facility = dplyr::if_else(
      num_orders == 0 & !is.na(shop),
      paste0(last_order_facility, ": ", shop),
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
