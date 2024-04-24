# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "blaze_employees", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("employees", paste(org, store, sep = "_"), "blaze")

# EXTRACT -----------------------------------------------------------------
employees <- hcablaze::extractBlaze(json, "employees")
employees$run_date_utc <- lubridate::now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
employees <- pipelinetools::check_cols(employees, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, employees, append = TRUE)
hcaconfig::dbd(pg)
