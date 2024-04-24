box::use(
  DBI[dbListFields, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcagreenbits[extract_gb_employees],
  lubridate[now],
  pipelinetools[rd_raw_archive, check_cols]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "gb_employees", sep = "_")

(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))

# READ --------------------------------------------------------------------
json <- rd_raw_archive("employees", paste(org, store, sep = "_"), "greenbits")

# EXTRACT -----------------------------------------------------------------
employees <- extract_gb_employees(json)
employees$run_date_utc <- now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- dbc(db_cfg, "cabbage_patch")
employees <- check_cols(employees, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, employees, append = TRUE)
dbd(pg)
