box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcasquare[extract_customers_segments],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "square_customers_segments", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("customers_segments", paste(org, store, sep = "_"), "square")

# EXTRACT -----------------------------------------------------------------
customers_segments <- extract_customers_segments(json)
customers_segments <- mutate(customers_segments, run_date_utc = now("UTC"))

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
customers_segments <- check_cols(customers_segments, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, customers_segments, append = TRUE)
dbd(pg)
