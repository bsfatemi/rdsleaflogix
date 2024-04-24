box::use(
  pipelinetools[rd_raw_archive, check_cols],
  hcaproteus[extract_proteus_customers],
  hcaconfig[dbc, dbd],
  DBI[dbWriteTable, dbListFields],
  lubridate[now],
  dplyr[mutate]
)
org <- "hiddentreasure"
out_table <- paste(org, "proteus_customers", sep = "_")
# read --------------------------------------------------------------------
js <- rd_raw_archive("customers", org, "proteus")

# query -------------------------------------------------------------------

customers <- extract_proteus_customers(js)
now_utc <- now(tzone = "UTC")
customers <- mutate(customers, run_date_utc = now_utc)
# write -------------------------------------------------------------------
pg <- dbc("prod2", "cabbage_patch")
customers <- check_cols(customers, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, customers, append = TRUE)
dbd(pg)
