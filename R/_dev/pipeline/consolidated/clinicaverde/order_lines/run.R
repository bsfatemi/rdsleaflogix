# Consolidated Order Lines
# init.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.
# Libraries ---------------------------------------------------------------

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  dplyr[select],
  hcaconfig[dbc, dbd, lookupOrgGuid, get_org_stores],
  hcapipelines[plIndex],
  pipelinetools[get_categories_mapping, db_read_table_unique],
  purrr[map_dfr]
)
source(
  "inst/pipeline/consolidated/clinicaverde/order_lines/src/build_akerna_order_lines.R",
  local = TRUE
)
org <- "clinicaverde"
org_uuid <- lookupOrgGuid(org)
stores <- get_org_stores(org_uuid)$facility
out_table <- plIndex()[short_name == org]$order_lines
db_env <- Sys.getenv("HCA_ENV", "prod2")

# Run ---------------------------------------------------------------------
cats_map <- select(get_categories_mapping(org_uuid), -run_date_utc)
pg <- dbc(db_env, "consolidated")
class_map <- select(dbReadTable(pg, "product_classes"), -run_date_utc)
dbd(pg)

# READ FROM DB-CB ---------------------------------------------------------
pg <- dbc(db_env, "cabbage_patch")
catalog <- db_read_table_unique(
  pg, paste(org, "all_akerna_catalog", sep = "_"), "id", "run_date_utc"
)
item_master <- db_read_table_unique(
  pg, paste(org, "all_akerna_item_masters", sep = "_"), "id", "run_date_utc"
)
order_lines <- map_dfr(stores, function(store) {
  order_details <- db_read_table_unique(
    pg, paste(org, store, "akerna_orders_products", sep = "_"), "products_id", "run_date_utc"
  )
  build_akerna_order_lines(order_details, item_master, catalog, class_map, cats_map, org)
})
dbd(pg)

# WRITE -------------------------------------------------------------------
con <- dbc(db_env, "consolidated")
dbBegin(con)
dbExecute(con, paste("TRUNCATE TABLE", paste0(org, "_order_lines")))
dbWriteTable(con, paste0(org, "_order_lines"), order_lines, append = TRUE)
dbCommit(con)
dbd(con)
