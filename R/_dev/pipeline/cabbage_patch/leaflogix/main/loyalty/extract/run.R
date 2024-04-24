box::use(
  DBI[dbListFields, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcaleaflogix[extract_loyalty],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "ll_loyalty", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("loyalty", paste(org, store, sep = "_"), "leaflogix")

# EXTRACT -----------------------------------------------------------------
loyalty <- extract_loyalty(json)
loyalty$run_date_utc <- now(tzone = "UTC")

# LAND --------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
loyalty <- check_cols(loyalty, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, loyalty, append = TRUE)
dbd(pg)
