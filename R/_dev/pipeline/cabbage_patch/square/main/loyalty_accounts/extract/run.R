box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcasquare[extract_loyalty_accounts],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "square_loyalty_accounts", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("loyalty_accounts", paste(org, store, sep = "_"), "square")

# EXTRACT -----------------------------------------------------------------
loyalty_accounts <- extract_loyalty_accounts(json)
loyalty_accounts <- mutate(loyalty_accounts, run_date_utc = now("UTC"))

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
loyalty_accounts <- check_cols(loyalty_accounts, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, loyalty_accounts, append = TRUE)
dbd(pg)
