# Libraries.
box::use(
  DBI[dbExecute, dbWriteTable],
  hcaconfig[dbc, dbd],
  hcatreez[getHCAKey],
  lubridate[minutes, now],
  purrr[map_dfr]
)
source("inst/pipeline/cabbage_patch/treez2-clients/checked_in_customers/init.R")

# Vars --------------------------------------------------------------------
# A checked in customer, would be a customer whose last visit was since these minutes.
last_check_in <- minutes(30)

# Run ---------------------------------------------------------------------
start <- Sys.time()
credentials <- get_treez_credentials()
client_id <- getHCAKey()
checked_in <- map_dfr(seq_len(nrow(credentials)), function(i) {
  store_credentials <- credentials[i, ]
  print(paste0("Querying for ", store_credentials$disp_name))
  # Query since this date.
  eob <- now(store_credentials$org_tz) - last_check_in
  # Perform the API query.
  get_checked_in(eob, store_credentials, client_id)
})
checked_in$run_date_utc <- now()
end <- Sys.time()
print(end - start)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
dbExecute(pg, paste("TRUNCATE TABLE", "treez_checked_in_customers"))
dbWriteTable(pg, "treez_checked_in_customers", checked_in, append = TRUE)
dbd(pg)
