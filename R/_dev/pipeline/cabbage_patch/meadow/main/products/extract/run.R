box::use(
  DBI[dbListFields, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcameadow[extract_products],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "meadow_products", sep = "_")

# READ --------------------------------------------------------------------
json_files <- rd_raw_archive("products", paste(org, store, sep = "_"), "meadow")

# EXTRACT -----------------------------------------------------------------
products <- extract_products(json_files)$products
products$run_date_utc <- now("UTC")

# LAND --------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
products <- check_cols(products, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, products, append = TRUE)
dbd(pg)
