box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcadispense[extract_carts],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "dispense_carts", sep = "_")
out_table_ci <- paste(org, store, "dispense_carts_items", sep = "_")

# READ --------------------------------------------------------------------
json_files <- rd_raw_archive("carts", paste(org, store, sep = "_"), "dispense")

# EXTRACT -----------------------------------------------------------------
carts <- extract_carts(json_files)
now_utc <- now("UTC")
carts_items <- mutate(carts$carts_items, run_date_utc = !!now_utc)
carts <- mutate(carts$carts, run_date_utc = !!now_utc)

# LAND --------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
carts <- check_cols(carts, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, carts, append = TRUE)
carts_items <- check_cols(carts_items, dbListFields(pg, out_table_ci))
dbWriteTable(pg, out_table_ci, carts_items, append = TRUE)
dbd(pg)
