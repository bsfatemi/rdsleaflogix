org <- "amuse" # actually store, parent org is amuse
facility <- "lux"
org_uuid <- "f0d2060d-9f1d-4d66-93d4-f106638aea91"

# READ --------------------------------------------------------------------
js <- pipelinetools::rd_raw_archive(
  dsName = "products",
  orgName = paste0(org, "_", facility),
  srcName = "treez2-clients",
  rawExt_d = "/mnt/data/pipeline/raw/"
)

# EXTRACT -----------------------------------------------------------------

out <- hcatreez::extractTreez(js, "products")
out$orguuid <- org_uuid
out$run_date_utc <- lubridate::now("UTC")
# LAND --------------------------------------------------------------------
pg <- hcaconfig::dbc("prod2", "cabbage_patch")
ex <- DBI::dbListFields(pg, paste0(org, "_", facility, "_trz2_products"))
final <- hcablaze::chkBlzCols(out, ex)
DBI::dbWriteTable(pg, paste0(org, "_", facility, "_trz2_products"), final, append = TRUE)
hcaconfig::dbd(pg)
