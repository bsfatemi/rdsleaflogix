box::use(
  DBI[dbListFields, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcagreenbits[extract_gb_product_types],
  lubridate[now],
  pipelinetools[rd_raw_archive, check_cols]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "gb_product_types", sep = "_")

(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))

# READ --------------------------------------------------------------------
json <- rd_raw_archive("product_types", paste(org, store, sep = "_"), "greenbits")

# EXTRACT -----------------------------------------------------------------
product_types <- extract_gb_product_types(json)
product_types$run_date_utc <- now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- dbc(db_cfg, "cabbage_patch")
product_types <- check_cols(product_types, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, product_types, append = TRUE)
dbd(pg)
