box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcabudee[extract_deliveries],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "budee_deliveries", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("deliveries", paste(org, store, sep = "_"), "budee")

# EXTRACT -----------------------------------------------------------------
deliveries <- extract_deliveries(json)
deliveries <- mutate(deliveries, run_date_utc = now("UTC"))

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
deliveries <- check_cols(deliveries, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, deliveries, append = TRUE)
dbd(pg)
