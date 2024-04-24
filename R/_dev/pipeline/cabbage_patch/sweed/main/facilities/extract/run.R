# VARS --------------------------------------------------------------------
org <- args$org
out_table <- paste(org, "sweed_facilities", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("data", org, "sweed")

# EXTRACT -----------------------------------------------------------------
facilities <- hcasweed::extract_facilities(json)
facilities$run_date_utc <- lubridate::now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
facilities <- pipelinetools::check_cols(facilities, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, facilities, append = TRUE)
hcaconfig::dbd(pg)
