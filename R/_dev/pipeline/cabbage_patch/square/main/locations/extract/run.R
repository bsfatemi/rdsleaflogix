box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcasquare[extract_locations],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "square_locations", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("locations", paste(org, store, sep = "_"), "square")

# EXTRACT -----------------------------------------------------------------
locations <- extract_locations(json)
locations <- mutate(locations, run_date_utc = now("UTC"))

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
locations <- check_cols(locations, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, locations, append = TRUE)
dbd(pg)
