# VARS --------------------------------------------------------------------
org <- args$org
out_table <- paste(org, "klaviyo_members_profiles", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("members_profiles", org, "klaviyo")
# Remove errored queries.
json <- json[!sapply(json, is.null)]

# EXTRACT -----------------------------------------------------------------
members_profiles <- hcaklaviyo::extract_profile(json)
members_profiles$run_date_utc <- lubridate::now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
members_profiles <- pipelinetools::check_cols(members_profiles, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, members_profiles, append = TRUE)
hcaconfig::dbd(pg)
