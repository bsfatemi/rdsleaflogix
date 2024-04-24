# Source ------------------------------------------------------------------
source("inst/pipeline/appdata/clair/customer_vip_points/init.R")
source("inst/pipeline/appdata/clair/customer_vip_points/src/build_vip_points.R")

# Vars --------------------------------------------------------------------
out_table <- "clair.customer_vip_points"

# Read --------------------------------------------------------------------
# cfg to connect in `hcaconfig::dbc`, "prod2" by default if "HCA_ENV" not set.
(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))
cn <- hcaconfig::dbc(db_cfg, "integrated")
res <- DBI::dbSendQuery(
  cn,
  "
  SELECT
    org_uuid, customer_id, phone, order_id, pos_product_id, first_name, last_name, birthday,
    order_time_utc, product_qty, item_subtotal, order_disc, order_tot
  FROM population2
  "
)
population <- data.table::as.data.table(DBI::dbFetch(res, 0))
lim <- 2^21
iter <- 0
while (TRUE) {
  iter <- iter + 1
  ss <- DBI::dbFetch(res, lim)
  data.table::setDT(ss)
  population <- data.table::rbindlist(list(population, ss))
  if (nrow(ss) < lim) {
    break
  }
}
DBI::dbClearResult(res)
hcaconfig::dbd(cn)

# Run ---------------------------------------------------------------------
vip_points <- build_customer_vip_points(population)

# Write -------------------------------------------------------------------
cn <- hcaconfig::dbc(db_cfg, "appdata")
DBI::dbExecute(cn, paste("TRUNCATE TABLE", DBI::SQL(out_table)))
DBI::dbWriteTable(cn, DBI::SQL(out_table), vip_points, append = TRUE)
hcaconfig::dbd(cn)
