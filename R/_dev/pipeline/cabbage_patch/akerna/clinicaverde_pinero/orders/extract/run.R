
# VARS --------------------------------------------------------------------
org <- "clinicaverde"
store <- "pinero"
raw_dir <- paste(org, store, sep = "_")
out_table <- paste(raw_dir, "akerna_orders", sep = "_")
js <- pipelinetools::rd_raw_archive("orders", raw_dir, "akerna")

# RUN -------------------------------------------------------------------------
orders <- hcaakerna::extract_akerna_orders(js)
orders$run_date_utc <- lubridate::now("UTC")

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
ex <- DBI::dbListFields(pg, out_table)
orders <- pipelinetools::check_cols(orders, ex)
DBI::dbWriteTable(pg, out_table, orders, append = TRUE)
hcaconfig::dbd(pg)
