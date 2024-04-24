# VARS --------------------------------------------------------------------
org <- args$org
out_table <- paste(org, "klaviyo_opt_outs", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("opt_outs", org, "klaviyo")

# EXTRACT -----------------------------------------------------------------
opt_outs <- hcaklaviyo::extract_global_exclusions_and_unsubscribes(json)
opt_outs$run_date_utc <- lubridate::now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
opt_outs <- pipelinetools::check_cols(opt_outs, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, opt_outs, append = TRUE)
hcaconfig::dbd(pg)
