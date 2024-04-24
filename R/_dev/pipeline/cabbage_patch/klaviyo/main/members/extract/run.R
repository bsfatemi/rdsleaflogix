# VARS --------------------------------------------------------------------
org <- args$org
out_table <- paste(org, "klaviyo_members", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("members", org, "klaviyo")

# EXTRACT -----------------------------------------------------------------
members <- hcaklaviyo::extract_list_and_segment_members(json)
members <- dplyr::distinct(members)
members$run_date_utc <- lubridate::now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
members <- pipelinetools::check_cols(members, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, members, append = TRUE)
hcaconfig::dbd(pg)
