org <- "clinicaverde"

js <- pipelinetools::rd_raw_archive("catalog", paste0(org, "_all"), "akerna")
# RUN -------------------------------------------------------------------------
catalog <- hcaakerna::extract_akerna_catalog(js)
catalog$run_date_utc <- lubridate::now("UTC")
catalog$primary_image_urls <- as.character(catalog$primary_image_urls)
catalog$weight_prices <- as.character(catalog$weight_prices)

# ARCHIVE -----------------------------------------------------------------
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
ex <- DBI::dbListFields(pg, paste0(org, "_all_akerna_catalog"))
catalog <- pipelinetools::check_cols(catalog, ex)
DBI::dbWriteTable(pg, paste0(org, "_all_akerna_catalog"), catalog, append = TRUE)
hcaconfig::dbd(pg)
