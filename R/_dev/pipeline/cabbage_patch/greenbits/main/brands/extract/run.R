box::use(
  DBI[dbListFields, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcagreenbits[extract_gb_brands],
  lubridate[now],
  pipelinetools[rd_raw_archive, check_cols]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "gb_brands", sep = "_")

(db_cfg <- Sys.getenv("HCA_ENV", "prod2"))

# READ --------------------------------------------------------------------
json <- rd_raw_archive("brands", paste(org, store, sep = "_"), "greenbits")

# EXTRACT -----------------------------------------------------------------
brands <- extract_gb_brands(json)
brands$run_date_utc <- now(tzone = "UTC")

# WRITE -------------------------------------------------------------------
pg <- dbc(db_cfg, "cabbage_patch")
brands <- check_cols(brands, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, brands, append = TRUE)
dbd(pg)
