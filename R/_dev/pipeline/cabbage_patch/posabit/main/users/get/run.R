box::use(
  hcaposabit[get_users],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
api_token <- args$api_token

start_utc <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))

# QUERY -------------------------------------------------------------------
json <- get_users(api_token, start_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "users", paste(org, store, sep = "_"), "posabit")
