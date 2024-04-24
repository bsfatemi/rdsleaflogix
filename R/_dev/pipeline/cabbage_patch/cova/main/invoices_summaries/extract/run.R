box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcaconfig[dbc, dbd],
  hcacova[extract_invoices_summaries],
  lubridate[now],
  pipelinetools[check_cols, rd_raw_archive]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "cova_invoices_summaries", sep = "_")

# READ --------------------------------------------------------------------
json <- rd_raw_archive("invoices_summaries", paste(org, store, sep = "_"), "cova")

# EXTRACT -----------------------------------------------------------------
invoices_summaries <- extract_invoices_summaries(json)
invoices_summaries <- mutate(invoices_summaries, run_date_utc = now("UTC"))

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
invoices_summaries <- check_cols(invoices_summaries, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, invoices_summaries, append = TRUE)
dbd(pg)
