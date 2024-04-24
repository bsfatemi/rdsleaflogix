# VARS --------------------------------------------------------------------
org <- args$org
out_table <- paste(org, "onfleet_tasks", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("tasks", org, "onfleet")

# EXTRACT -----------------------------------------------------------------
tasks <- hcaonfleet::extract_tasks(json)
tasks$run_date_utc <- lubridate::now("UTC")

# LAND --------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
tasks <- pipelinetools::check_cols(tasks, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, tasks, append = TRUE)
hcaconfig::dbd(pg)
