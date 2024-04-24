box::use(
  DBI[dbListFields, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcaflowhub[extract_fh_products],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "flowhub_products", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("products", paste(org, store, sep = "_"), "flowhub")

# EXTRACT -----------------------------------------------------------------
products <- extract_fh_products(json)
products$run_date_utc <- now("UTC")

# STORE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
products <- check_cols(products, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, products, append = TRUE)
dbd(pg)
