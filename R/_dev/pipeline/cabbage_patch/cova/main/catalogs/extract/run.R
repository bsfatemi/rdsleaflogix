box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcacova[extract_catalogs],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "cova_catalogs", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("catalogs", paste(org, store, sep = "_"), "cova")

# EXTRACT -----------------------------------------------------------------
catalogs <- extract_catalogs(json)
catalogs <- mutate(catalogs, run_date_utc = now("UTC"))

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
catalogs <- check_cols(catalogs, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, catalogs, append = TRUE)
dbd(pg)
