# VARS --------------------------------------------------------------------
org <- args$org
out_table <- paste(org, "magento_categories", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("categories", paste(org, sep = "_"), "magento")

# EXTRACT -----------------------------------------------------------------
categories <- hcamagento::extract_categories(json)
categories$run_date_utc <- lubridate::now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
categories <- pipelinetools::check_cols(categories, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, categories, append = TRUE)
hcaconfig::dbd(pg)
