box::use(
  hcabudee[get_deliveries],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
apikey <- args$apikey

end_utc <- today("UTC")
start_utc <- end_utc - days(Sys.getenv("PREVIOUS_DAYS", 1))

# QUERY -------------------------------------------------------------------
json <- get_deliveries(apikey, start_utc, end_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "deliveries", paste(org, store, sep = "_"), "budee")
