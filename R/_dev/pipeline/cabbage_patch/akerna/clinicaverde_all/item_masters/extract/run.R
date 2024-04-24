
# VARS --------------------------------------------------------------------
org <- "clinicaverde"
out_table <- paste(org, "all_akerna_item_masters", sep = "_")
js <- pipelinetools::rd_raw_archive("item_masters", paste0(org, "_all"), "akerna")

# RUN -------------------------------------------------------------------------
item_masters <- hcaakerna::extract_akerna_orders(js)
item_masters$run_date_utc <- lubridate::now("UTC")

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
ex <- DBI::dbListFields(pg, out_table)
item_masters <- pipelinetools::check_cols(item_masters, ex)
DBI::dbWriteTable(pg, out_table, item_masters, append = TRUE)
hcaconfig::dbd(pg)
