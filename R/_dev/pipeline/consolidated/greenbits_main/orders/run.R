# Consolidated Orders
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

# Vars ------------------------------------------------------------------
org <- args$org_short_name
stores <- args$stores_short_names[[1]]
stores_full_names <- args$stores_full_names[[1]]
stores_uuids <- args$stores_uuids[[1]]
out_table <- args$orders

box::use(
  hcaconfig[dbc, dbd],
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbWriteTable],
  purrr[map_dfr],
  pipelinetools[db_read_table_unique],
  hcagreenbits[build_gb_orders],
  dplyr[mutate]
)


(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))
org_tz <- hcaconfig::orgTimeZone(hcaconfig::lookupOrgGuid(org))
# Run ---------------------------------------------------------------------
pg <- hcaconfig::dbc(db_cfg, "cabbage_patch")
orders <- purrr::map_dfr(seq_along(stores), function(i) {
  store <- stores[[i]]

  orders <- db_read_table_unique(
    pg,
    paste(org, store, "gb_orders", sep = "_"), "id", "run_date_utc"
  )

  discounts <- db_read_table_unique(
    pg,
    paste(org, store, "gb_order_line_items_discounts", sep = "_"), "id", "run_date_utc"
  )

  taxes <- db_read_table_unique(
    pg,
    paste(org, store, "gb_order_line_items_tax_breakdowns", sep = "_"), "id", "run_date_utc"
  )

  employees <- db_read_table_unique(pg,
    paste(org, store, "gb_employees", sep = "_"), "id", "run_date_utc",
    columns = c("id", "name", "run_date_utc")
  )

  order_detail_ids <- db_read_table_unique(pg,
    paste(org, store, "gb_order_line_items", sep = "_"), "id", "run_date_utc",
    columns = c("id, order_id")
  )

  build_gb_orders(
    org, stores_full_names[[i]], store, stores_uuids[[i]], org_tz,
    orders, discounts, taxes, employees, order_detail_ids
  ) |>
    mutate(customer_id = paste(store, customer_id, sep = "-"))
})
hcaconfig::dbd(pg)

# Write -------------------------------------------------------------------

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbCommit(pg)
dbd(pg)
