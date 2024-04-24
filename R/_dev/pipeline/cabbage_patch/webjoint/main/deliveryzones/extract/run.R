box::use(
  DBI[dbWriteTable, dbListFields],
  hcaconfig[dbc, dbd],
  hcawebjoint[extract_deliveryzones],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "wj_deliveryzones", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("deliveryzones", paste(org, store, sep = "_"), "webjoint")

# EXTRACT -----------------------------------------------------------------
deliveryzones <- extract_deliveryzones(json)
deliveryzones$run_date_utc <- now("UTC")

# LAND --------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
deliveryzones <- check_cols(deliveryzones, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, deliveryzones, append = TRUE)
dbd(pg)
