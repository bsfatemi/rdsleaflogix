# Allow absolute module imports (relative to the project root).
options(box.path = getwd())

# Libraries.
box::use(
  DBI[dbExecute, dbWriteTable],
  hcaconfig[dbc, dbd],
  lubridate[minutes, now],
  purrr[map_dfr],
  utils[URLencode]
)
source("inst/pipeline/cabbage_patch/blaze/checked_in_customers/init.R")
source("inst/pipeline/cabbage_patch/blaze/checked_in_customers/src/anonymize_demoorg.R")

# Vars --------------------------------------------------------------------
# A checked in customer, would be a customer whose last visit was since these minutes.
last_check_in <- minutes(30)

# Run ---------------------------------------------------------------------
start <- Sys.time()
credentials <- get_blaze_credentials()
checked_in <- map_dfr(seq_len(nrow(credentials)), function(i) {
  store_credentials <- credentials[i, ]
  print(paste0("Querying for ", store_credentials$facility))
  # Query since this date.
  eob <- now(store_credentials$org_tz) - last_check_in
  # Perform the API query.
  ci <- get_checked_in(eob, store_credentials)
  # Anonymization for DemoOrg.
  if (store_credentials$org_uuid == "fcfaa275-9529-490d-a156-858892aaf365") {
    ci <- anonymize_demoorg("demoorg", store_credentials, ci)
  }
  ci
})
checked_in$run_date_utc <- now()
end <- Sys.time()
print(end - start)

# Write -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
dbExecute(pg, paste("TRUNCATE TABLE", "blaze_checked_in_customers"))
dbWriteTable(pg, "blaze_checked_in_customers", checked_in, append = TRUE)
dbd(pg)
