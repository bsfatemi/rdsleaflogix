box::use(
  hcabudee[get_depots],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
apikey <- args$apikey

start_utc <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))

# QUERY -------------------------------------------------------------------
json <- get_depots(apikey, start_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "depots", paste(org, store, sep = "_"), "budee")
