# Libraries.
box::use(
  DBI[dbExecute, dbWriteTable],
  hcaconfig[dbc, dbd],
  lubridate[now],
  purrr[map_dfr]
)
source("inst/pipeline/cabbage_patch/leaflogix/checked_in_customers/init.R")

# Run ---------------------------------------------------------------------
start <- Sys.time()
credentials <- get_leaflogix_credentials()
checked_in <- map_dfr(seq_len(nrow(credentials)), function(i) {
  store_credentials <- credentials[i, ]
  print(paste0("Querying for ", store_credentials$facility))
  # Perform the API query.
  get_checked_in(store_credentials)
})
checked_in$run_date_utc <- now()
end <- Sys.time()
print(end - start)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
dbExecute(pg, paste("TRUNCATE TABLE", "leaflogix_checked_in_customers"))
dbWriteTable(pg, "leaflogix_checked_in_customers", checked_in, append = TRUE)
dbd(pg)
