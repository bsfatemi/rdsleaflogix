box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcalightspeed[extract_employees],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "ls_employees", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("employees", paste(org, store, sep = "_"), "lightspeed")

# EXTRACT -----------------------------------------------------------------
employees <- extract_employees(json)
employees$run_date_utc <- now("UTC")

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
employees <- check_cols(employees, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, employees, append = TRUE)
dbd(pg)
