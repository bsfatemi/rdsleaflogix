box::use(
  hcatreez[getPageTreez],
  lubridate[days, today],
  pipelinetools[storeExternalRaw],
  utils[URLencode]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
client_id <- args$client_id
apikey <- args$apikey
dispensary_name <- args$dispensary_name

# Query since this date.
eob_utc <- URLencode(
  format(today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1)), "%Y-%m-%dT%H:%M:%S.000-07:00"),
  reserved = TRUE
)
endpt <- paste0("/customer/lastUpdated/after/", eob_utc)

# QUERY -------------------------------------------------------------------
json <- getPageTreez(apikey, client_id, endpt, dispensary_name)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "customers", paste(org, store, sep = "_"), "treez2-clients")
