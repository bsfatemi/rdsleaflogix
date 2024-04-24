box::use(
  DBI[dbListFields, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcaleaflogix[extract_employees],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "ll_employees", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("employees", paste(org, store, sep = "_"), "leaflogix")

# EXTRACT -----------------------------------------------------------------
employees <- extract_employees(json)
employees$run_date_utc <- now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
employees <- check_cols(employees, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, employees, append = TRUE)
dbd(pg)
