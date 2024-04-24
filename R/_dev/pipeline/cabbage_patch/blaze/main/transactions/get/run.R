# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
uuid <- args$uuid
apikey <- args$apikey

#PREVIOUS_DAYS cannot be > 7 due to Blaze API restrictions
orderdays <- Sys.getenv("PREVIOUS_DAYS", 1)
start_utc <- lubridate::as_datetime(lubridate::today("UTC")) - lubridate::days(orderdays)
endpt <- "/transactions/days"

# QUERY -------------------------------------------------------------------
json <- hcablaze::getBlaze(
  endpt, "skip", apikey, org,
  startDate = format(start_utc, "%m/%d/%Y"),
  days = orderdays
)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "transactions", paste(org, store, sep = "_"), "blaze")
