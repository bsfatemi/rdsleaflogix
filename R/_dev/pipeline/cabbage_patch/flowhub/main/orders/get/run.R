box::use(
  hcaflowhub[get_orders],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
apikey <- args$apikey
clientid <- args$clientid
loc_id <- args$loc_id

# Query for this dates period.
end_date <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))
start_date <- end_date - days(1)

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/flowhub/", org, "_", store), showWarnings = FALSE)

# QUERY -------------------------------------------------------------------
json <- get_orders(apikey, clientid, end_date, loc_id, start_date)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "orders", paste(org, store, sep = "_"), "flowhub")
