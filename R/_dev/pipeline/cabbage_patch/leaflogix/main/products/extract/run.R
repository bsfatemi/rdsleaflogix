box::use(
  DBI[dbListFields, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcaleaflogix[extract_products],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "ll_products", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("products", paste(org, store, sep = "_"), "leaflogix")

# EXTRACT -----------------------------------------------------------------
products <- extract_products(json)
products$run_date_utc <- now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
products <- check_cols(products, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, products, append = TRUE)
dbd(pg)
