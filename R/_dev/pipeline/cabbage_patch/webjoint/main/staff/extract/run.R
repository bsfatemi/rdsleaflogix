box::use(
  DBI[dbWriteTable, dbListFields],
  hcaconfig[dbc, dbd],
  hcawebjoint[extract_staff],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "wj_staff", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("staff", paste(org, store, sep = "_"), "webjoint")

# EXTRACT -----------------------------------------------------------------
staff <- extract_staff(json)
staff$run_date_utc <- now("UTC")

# LAND --------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
staff <- check_cols(staff, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, staff, append = TRUE)
dbd(pg)
