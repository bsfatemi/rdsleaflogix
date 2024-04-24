# READ --------------------------------------------------------------------
js <- pipelinetools::rd_raw_archive("customers", "amuse_lux", "treez2-clients")

# EXTRACT -----------------------------------------------------------------
out <- hcatreez::extractTreez(js, "customers")
out$run_date_utc <- lubridate::now("UTC")

# LAND --------------------------------------------------------------------
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
ex <- DBI::dbListFields(pg, "amuse_lux_trz2_customers")
final <- hcablaze::chkBlzCols(out, ex)
DBI::dbWriteTable(pg, "amuse_lux_trz2_customers", final, append = TRUE)
hcaconfig::dbd(pg)
