# VARS --------------------------------------------------------------------
org <- args$org
out_table <- paste(org, "klaviyo_lists", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("lists", org, "klaviyo")

# EXTRACT -----------------------------------------------------------------
lists <- hcaklaviyo::extract_lists(json)
lists$run_date_utc <- lubridate::now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
lists <- pipelinetools::check_cols(lists, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, lists, append = TRUE)
hcaconfig::dbd(pg)
