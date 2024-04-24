box::use(
  DBI[dbListFields, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcaleaflogix[extract_customers],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "ll_customers", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("customers", paste(org, store, sep = "_"), "leaflogix")

# EXTRACT -----------------------------------------------------------------
customers <- extract_customers(json)
customers$run_date_utc <- now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
customers <- check_cols(customers, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbd(pg)
