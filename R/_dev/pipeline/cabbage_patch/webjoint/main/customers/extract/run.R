box::use(
  DBI[dbWriteTable, dbListFields],
  hcaconfig[dbc, dbd],
  hcawebjoint[extract_customers],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "wj_customers", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("customers", paste(org, store, sep = "_"), "webjoint")

# EXTRACT -----------------------------------------------------------------
customers <- extract_customers(json)
customers$run_date_utc <- now("UTC")

# LAND --------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
customers <- check_cols(customers, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbd(pg)
