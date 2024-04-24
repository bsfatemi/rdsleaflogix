# Consolidated order_lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

# Vars ------------------------------------------------------------------
org <- args$org_short_name
org_uuid <- args$org_uuid
stores <- args$stores_short_names[[1]]
out_table <- args$order_lines

box::use(
  hcaconfig[dbc, dbd],
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  purrr[map_dfr],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  hcagreenbits[build_gb_order_lines],
  dplyr[select, mutate]
)


# Run ---------------------------------------------------------------------
(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))
cats_map <- select(get_categories_mapping(org_uuid), -run_date_utc)
pg <- dbc(db_cfg, "consolidated")
class_map <- select(dbReadTable(pg, "product_classes"), -run_date_utc)
dbd(pg)

pg <- dbc(db_cfg, "cabbage_patch")
order_lines <- map_dfr(seq_along(stores), function(i) {
  store <- stores[[i]]

  customer_ids <- db_read_table_unique(pg,
    paste(org, store, "gb_orders", sep = "_"), "id", "run_date_utc",
    columns = c("id", "customer_id", "run_date_utc")
  )

  cart_items <- db_read_table_unique(
    pg,
    paste(org, store, "gb_order_line_items", sep = "_"), "id", "run_date_utc"
  )

  discounts <- db_read_table_unique(
    pg,
    paste(org, store, "gb_order_line_items_discounts", sep = "_"), "id", "run_date_utc"
  )

  taxes <- db_read_table_unique(
    pg,
    paste(org, store, "gb_order_line_items_tax_breakdowns", sep = "_"), "id", "run_date_utc"
  )

  inventory_items <- db_read_table_unique(
    pg,
    paste(org, store, "gb_inventory_items", sep = "_"), "id", "run_date_utc"
  )

  employees <- db_read_table_unique(pg,
    paste(org, store, "gb_employees", sep = "_"), "id", "run_date_utc",
    columns = c("id", "name")
  )

  brands <- db_read_table_unique(
    pg,
    paste(org, store, "gb_brands", sep = "_"), "id", "run_date_utc"
  )

  build_gb_order_lines(
    org, customer_ids, discounts, taxes, cart_items, inventory_items, brands, cats_map, class_map
  ) |>
    mutate(customer_id = paste(store, customer_id, sep = "-"))
})
hcaconfig::dbd(pg)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
