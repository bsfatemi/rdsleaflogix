box::use(
  DBI[dbListFields, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcatreez[extractTreez],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "trz2_products", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("products", paste(org, store, sep = "_"), "treez2-clients")

# EXTRACT -----------------------------------------------------------------
# If all values in json are empty, then skip extraction.
if (!all(json == "{\"data\":{\"product_list\":[]}}")) {
  products <- extractTreez(json, "products")
  products$run_date_utc <- now("UTC")

  # LAND --------------------------------------------------------------------
  pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
  products <- check_cols(products, dbListFields(pg, out_table))
  dbWriteTable(pg, out_table, products, append = TRUE)
  dbd(pg)
}
