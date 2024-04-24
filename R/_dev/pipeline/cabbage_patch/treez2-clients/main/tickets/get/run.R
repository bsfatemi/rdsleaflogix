box::use(
  hcatreez[getPageTreez],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
client_id <- args$client_id
apikey <- args$apikey
dispensary_name <- args$dispensary_name

# Query for this exact date.
eob_utc <- format(today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1)), "%Y-%m-%d")
endpt <- paste0("/ticket/closedate/", eob_utc)

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/treez2-clients/", org, "_", store), showWarnings = FALSE)

# QUERY -------------------------------------------------------------------
json <- getPageTreez(apikey, client_id, endpt, dispensary_name)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "tickets", paste(org, store, sep = "_"), "treez2-clients")
