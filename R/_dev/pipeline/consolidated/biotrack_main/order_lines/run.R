# Consolidated Order Lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbWriteTable],
  dplyr[select],
  hcabiotrack[build_biotrack_order_lines],
  hcaconfig[dbc, dbd],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------
org <- args$org_short_name
org_uuid <- args$org_uuid
stores <- args$stores_short_names[[1]]
out_table <- args$order_lines

# Run ---------------------------------------------------------------------
cats_map <- get_categories_mapping(org_uuid) |> select(-run_date_utc)
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
class_map <- dbGetQuery(pg, "SELECT raw_product_class, product_class FROM product_classes")
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
order_lines <- map_dfr(stores, function(store) {
  order_lines <- db_read_table_unique(
    pg, paste(org, store, "biotrack_order_lines", sep = "_"), "id", "run_date_utc",
    columns = c(
      "id", "price", "price_post_discount", "price_post_everything", "product_name", "productid",
      "raw_category_name", "run_date_utc", "strain", "tax_collected", "ticketid", "weight"
    )
  )
  build_biotrack_order_lines(order_lines, cats_map, class_map, org)
})
dbd(pg)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", out_table))
dbWriteTable(pg, out_table, order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
