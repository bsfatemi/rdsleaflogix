box::use(
  DBI[dbListFields, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcaposabit[extract_customers],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "posabit_customers", sep = "_")

# READ --------------------------------------------------------------------
json_files <- rd_raw_archive("customers", paste(org, store, sep = "_"), "posabit")

# EXTRACT -----------------------------------------------------------------
customers <- extract_customers(json_files)
customers$run_date_utc <- now("UTC")

# LAND --------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
customers <- check_cols(customers, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbd(pg)
