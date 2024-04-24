box::use(
  pipelinetools[rd_raw_archive, check_cols],
  hcaproteus[extract_proteus_products],
  hcaconfig[dbc, dbd],
  DBI[dbWriteTable, dbListFields],
  lubridate[now],
  dplyr[mutate]
)
org <- "superiororganics"
out_table <- paste(org, "proteus_products", sep = "_")
# read --------------------------------------------------------------------
js <- rd_raw_archive("products", org, "proteus")

# query -------------------------------------------------------------------

products <- extract_proteus_products(js)
now_utc <- now(tzone = "UTC")
products <- mutate(products, run_date_utc = now_utc)
# write -------------------------------------------------------------------
pg <- dbc("prod2", "cabbage_patch")
products <- check_cols(products, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, products, append = TRUE)
dbd(pg)
