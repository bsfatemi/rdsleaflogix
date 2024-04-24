# VARS --------------------------------------------------------------------
org <- args$org
out_table <- paste(org, "magento_products", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("products", paste(org, sep = "_"), "magento")

# EXTRACT -----------------------------------------------------------------
products <- hcamagento::extract_products(json)
products$run_date_utc <- lubridate::now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
products <- pipelinetools::check_cols(products, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, products, append = TRUE)
hcaconfig::dbd(pg)
