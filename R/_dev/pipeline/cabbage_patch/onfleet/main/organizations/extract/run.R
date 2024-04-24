# VARS --------------------------------------------------------------------
org <- args$org
out_table <- paste(org, "onfleet_organizations", sep = "_")

# READ --------------------------------------------------------------------
jsons <- pipelinetools::rd_raw_archive("organizations", org, "onfleet")

# EXTRACT -----------------------------------------------------------------
organizations <- hcaonfleet::extract_organizations(jsons)
organizations$run_date_utc <- lubridate::now("UTC")

# LAND --------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
organizations <- pipelinetools::check_cols(organizations, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, organizations, append = TRUE)
hcaconfig::dbd(pg)
