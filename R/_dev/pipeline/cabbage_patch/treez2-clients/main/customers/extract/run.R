# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "trz2_customers", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("customers", paste(org, store, sep = "_"), "treez2-clients")

# EXTRACT -----------------------------------------------------------------
customers <- hcatreez::extractTreez(json, "customers")
customers$run_date_utc <- lubridate::now("UTC")

# LAND --------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
customers <- pipelinetools::check_cols(customers, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, customers, append = TRUE)
hcaconfig::dbd(pg)
