box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcabudee[extract_depots],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "budee_depots", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("depots", paste(org, store, sep = "_"), "budee")

# EXTRACT -----------------------------------------------------------------
depots <- extract_depots(json)
depots <- mutate(depots, run_date_utc = now("UTC"))

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
depots <- check_cols(depots, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, depots, append = TRUE)
dbd(pg)
