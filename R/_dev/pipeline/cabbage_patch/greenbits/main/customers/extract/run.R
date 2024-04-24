box::use(
  DBI[dbListFields, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcagreenbits[extract_gb_customers],
  lubridate[now],
  pipelinetools[rd_raw_archive, check_cols]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "gb_customers", sep = "_")

(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))

# READ --------------------------------------------------------------------
json <- rd_raw_archive("customers", paste(org, store, sep = "_"), "greenbits")

# EXTRACT -----------------------------------------------------------------
customers <- extract_gb_customers(json)
customers$run_date_utc <- now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- dbc(db_cfg, "cabbage_patch")
customers <- check_cols(customers, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbd(pg)
