box::use(
  hcablaze[getBlaze],
  lubridate[as_datetime, days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
uuid <- args$uuid
apikey <- args$apikey

stop_utc <- as_datetime(today("UTC"))
start_utc <- stop_utc - days(Sys.getenv("PREVIOUS_DAYS", 1))
endpt <- "/products"

# QUERY -------------------------------------------------------------------
json <- getBlaze(
  endpt, "skip", apikey, org,
  startDate = format(start_utc, "%m/%d/%Y"),
  endDate = format(stop_utc, "%m/%d/%Y")
)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "products", paste(org, store, sep = "_"), "blaze")
