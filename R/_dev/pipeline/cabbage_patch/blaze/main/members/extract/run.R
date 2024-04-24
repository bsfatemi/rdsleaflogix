# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "blaze_members", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("members", paste(org, store, sep = "_"), "blaze")

# EXTRACT -----------------------------------------------------------------
members <- hcablaze::extractBlaze(json, "members")
members$run_date_utc <- lubridate::now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
members <- pipelinetools::check_cols(members, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, members, append = TRUE)
hcaconfig::dbd(pg)
