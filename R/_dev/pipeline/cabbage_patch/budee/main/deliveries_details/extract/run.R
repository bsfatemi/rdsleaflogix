box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcabudee[extract_deliveries_details],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "budee_deliveries_details", sep = "_")
out_table_i <- paste(org, store, "budee_deliveries_details_items", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("deliveries_details", paste(org, store, sep = "_"), "budee")

# EXTRACT -----------------------------------------------------------------
deliveries_details <- extract_deliveries_details(json)
now_utc <- now("UTC")
deliveries_details_items <- mutate(
  deliveries_details$deliveries_details_items,
  run_date_utc = now_utc
)
deliveries_details <- mutate(deliveries_details$deliveries_details, run_date_utc = now_utc)

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
deliveries_details <- check_cols(deliveries_details, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, deliveries_details, append = TRUE)
deliveries_details_items <- check_cols(deliveries_details_items, dbListFields(pg, out_table_i))
dbWriteTable(pg, out_table_i, deliveries_details_items, append = TRUE)
dbd(pg)
