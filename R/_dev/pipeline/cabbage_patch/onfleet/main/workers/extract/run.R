# VARS --------------------------------------------------------------------
org <- args$org
out_table <- paste(org, "onfleet_workers", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("workers", org, "onfleet")

# EXTRACT -----------------------------------------------------------------
workers <- hcaonfleet::extract_workers(json)
workers$run_date_utc <- lubridate::now("UTC")

# LAND --------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
workers <- pipelinetools::check_cols(workers, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, workers, append = TRUE)
hcaconfig::dbd(pg)
