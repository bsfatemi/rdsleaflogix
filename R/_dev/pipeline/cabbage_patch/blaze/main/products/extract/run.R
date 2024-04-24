# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "blaze_products", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("products", paste(org, store, sep = "_"), "blaze")

# EXTRACT -----------------------------------------------------------------
products <- hcablaze::extractBlaze(json, "products")
products$run_date_utc <- lubridate::now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
products <- pipelinetools::check_cols(products, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, products, append = TRUE)
hcaconfig::dbd(pg)
