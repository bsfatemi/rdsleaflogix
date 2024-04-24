box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcalightspeed[extract_items],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "ls_items", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("items", paste(org, store, sep = "_"), "lightspeed")

# EXTRACT -----------------------------------------------------------------
items <- extract_items(json)
items$run_date_utc <- now("UTC")

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
items <- check_cols(items, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, items, append = TRUE)
dbd(pg)
