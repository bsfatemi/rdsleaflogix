box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcasquare[extract_catalog],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table_i <- paste(org, store, "square_catalog_items", sep = "_")
out_table_iv <- paste(org, store, "square_catalog_item_variations", sep = "_")
out_table_c <- paste(org, store, "square_catalog_categories", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("catalog", paste(org, store, sep = "_"), "square")

# EXTRACT -----------------------------------------------------------------
catalog <- extract_catalog(json)
catalog_items <- mutate(catalog$catalog_items, run_date_utc = now("UTC"))
catalog_item_variations <- mutate(catalog$catalog_item_variations, run_date_utc = now("UTC"))
catalog_categories <- mutate(catalog$catalog_categories, run_date_utc = now("UTC"))

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
catalog_items <- check_cols(catalog_items, dbListFields(pg, out_table_i))
dbWriteTable(pg, out_table_i, catalog_items, append = TRUE)
catalog_item_variations <- check_cols(catalog_item_variations, dbListFields(pg, out_table_iv))
dbWriteTable(pg, out_table_iv, catalog_item_variations, append = TRUE)
catalog_categories <- check_cols(catalog_categories, dbListFields(pg, out_table_c))
dbWriteTable(pg, out_table_c, catalog_categories, append = TRUE)
dbd(pg)
