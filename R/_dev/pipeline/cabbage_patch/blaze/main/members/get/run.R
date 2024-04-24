# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
uuid <- args$uuid
apikey <- args$apikey

orderdays <- 1
start_utc <- lubridate::as_datetime(lubridate::today("UTC")) - lubridate::days(orderdays)
endpt <- "/members/days"

# QUERY -------------------------------------------------------------------
json <- hcablaze::getBlaze(
  endpt, "skip", apikey, org,
  startDate = format(as.numeric(start_utc) * 1000, scientific = FALSE),
  days = orderdays
)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "members", paste(org, store, sep = "_"), "blaze")
