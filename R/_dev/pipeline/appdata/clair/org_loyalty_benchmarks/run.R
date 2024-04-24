box::use(
  DBI[dbExecute, dbGetQuery, dbWriteTable, SQL],
  glue[glue],
  hcaconfig[dbc, dbd, orgIndex],
  purrr[map_dfr]
)

# Source ------------------------------------------------------------------
source("inst/pipeline/appdata/clair/org_loyalty_benchmarks/src/build_loyalty_benchmark.R")

# Vars --------------------------------------------------------------------
out_table <- "clair.org_loyalty_benchmarks"

# Read & Run --------------------------------------------------------------
orgGuids <- orgIndex()[, unique(org_uuid)]
query_str <- "
  SELECT customer_id, order_id, product_qty, item_total, item_discount
  FROM population2
  WHERE org_uuid = '{oid}' AND customer_id IS NOT NULL;
"
# cfg to connect in `hcaconfig::dbc`, "prod2" by default if "HCA_ENV" not set.
(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))
cn <- dbc(db_cfg, "integrated")
bmarks <- map_dfr(orgGuids, function(oid) {
  odt <- dbGetQuery(cn, SQL(glue(query_str)))
  build_loyalty_benchmark(oid, odt)
})
dbd(cn)
bmarks <- bmarks[!is.na(bmarks$spend_silver) & !is.na(bmarks$disc_silver), ]

# Write -------------------------------------------------------------------
cn <- dbc(db_cfg, "appdata")
dbExecute(cn, paste("TRUNCATE TABLE", SQL(out_table)))
dbWriteTable(cn, SQL(out_table), bmarks, append = TRUE)
dbd(cn)
