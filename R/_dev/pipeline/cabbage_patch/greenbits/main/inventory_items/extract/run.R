box::use(
  DBI[dbListFields, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcagreenbits[extract_gb_inventory_items],
  lubridate[now],
  pipelinetools[rd_raw_archive, check_cols]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "gb_inventory_items", sep = "_")

(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))

# READ --------------------------------------------------------------------
json <- rd_raw_archive("inventory_items", paste(org, store, sep = "_"), "greenbits")

# EXTRACT -----------------------------------------------------------------
inventory_items <- extract_gb_inventory_items(json)
inventory_items$run_date_utc <- now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- dbc(db_cfg, "cabbage_patch")
inventory_items <- check_cols(inventory_items, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, inventory_items, append = TRUE)
dbd(pg)
