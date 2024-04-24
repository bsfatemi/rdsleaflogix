box::use(
  hcasquare[get_locations, get_orders, extract_locations],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
access_token <- args$access_token

end_utc <- today("UTC")
start_utc <- end_utc - days(Sys.getenv("PREVIOUS_DAYS", 1))

# QUERY -------------------------------------------------------------------
location_ids <- unique(extract_locations(get_locations(access_token))$id)
json <- get_orders(access_token, location_ids, start_utc, end_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "orders", paste(org, store, sep = "_"), "square")
