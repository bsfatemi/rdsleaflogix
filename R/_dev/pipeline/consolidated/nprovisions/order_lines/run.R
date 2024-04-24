# Consolidated Order Lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[bind_rows, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd, lookupOrgGuid, get_org_stores],
  hcapipelines[plIndex],
  hcaposabit[build_posabit_order_lines],
  lubridate[now],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------
org <- "nprovisions"
org_uuid <- lookupOrgGuid(org)
stores <- get_org_stores(org_uuid)$facility
out_table <- plIndex()[short_name == org]$order_lines


# Run ---------------------------------------------------------------------
cats_map <- get_categories_mapping(org_uuid)
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
class_map <- dbReadTable(pg, "product_classes")
customers <- dbGetQuery(
  pg, paste0("SELECT DISTINCT treez_customer_id, customer_id FROM ", org, "_customers")
)
treez_order_lines <- dbReadTable(pg, "nprovisions_treez_order_lines")
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
posabit_order_lines <- map_dfr(stores, function(store) {
  orders <- db_read_table_unique(
    pg, paste(org, store, "posabit_orders", sep = "_"), "id", "run_date_utc"
  )
  order_items <- db_read_table_unique(
    pg, paste(org, store, "posabit_order_items", sep = "_"), "item_id", "run_date_utc"
  )
  build_posabit_order_lines(orders, order_items, cats_map, org)
})
dbd(pg)

treez_order_lines <- rename(treez_order_lines, treez_customer_id = customer_id) |>
  mutate(treez_customer_id = paste0("treez_", treez_customer_id)) |>
  left_join(customers, by = "treez_customer_id") |>
  select(-treez_customer_id)

# Join ------------------------------------------------------------------------
order_lines <- bind_rows(posabit_order_lines, treez_order_lines)
order_lines$run_date_utc <- now("UTC")

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
