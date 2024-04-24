# VARS --------------------------------------------------------------------
org <- args$org
out_table <- paste(org, "sweed_customers", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("data", org, "sweed")

# EXTRACT -----------------------------------------------------------------
customers <- hcasweed::extract_customers(json)
customers$run_date_utc <- lubridate::now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
customers <- pipelinetools::check_cols(customers, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, customers, append = TRUE)
hcaconfig::dbd(pg)
