# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "blaze_brands", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("brands", paste(org, store, sep = "_"), "blaze")

# EXTRACT -----------------------------------------------------------------
brands <- hcablaze::extractBlaze(json, "brands")
brands$run_date_utc <- lubridate::now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
brands <- pipelinetools::check_cols(brands, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, brands, append = TRUE)
hcaconfig::dbd(pg)
