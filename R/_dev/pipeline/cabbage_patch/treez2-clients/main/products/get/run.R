box::use(
  hcatreez[getProducts],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)
# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
client_id <- args$client_id
apikey <- args$apikey
dispensary_name <- args$dispensary_name

## Starts pulling products last updated after this time
eob_utc <- today() - days(Sys.getenv("PREVIOUS_DAYS", 1))

# QUERY -------------------------------------------------------------------
json <- getProducts(apikey, client_id, dispensary_name, eob_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "products", paste(org, store, sep = "_"), "treez2-clients")
