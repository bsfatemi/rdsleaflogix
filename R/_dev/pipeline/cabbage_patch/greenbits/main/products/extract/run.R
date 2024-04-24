box::use(
  DBI[dbListFields, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcagreenbits[extract_gb_products],
  lubridate[now],
  pipelinetools[rd_raw_archive, check_cols]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "gb_products", sep = "_")

(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))

# READ --------------------------------------------------------------------
json <- rd_raw_archive("products", paste(org, store, sep = "_"), "greenbits")

# EXTRACT -----------------------------------------------------------------
products <- extract_gb_products(json)
products$run_date_utc <- now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- dbc(db_cfg, "cabbage_patch")
products <- check_cols(products, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, products, append = TRUE)
dbd(pg)
