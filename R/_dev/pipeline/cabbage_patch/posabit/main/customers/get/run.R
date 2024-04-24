box::use(
  hcaposabit[get_customers],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
api_token <- args$api_token

start_utc <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/posabit/", org, "_", store), showWarnings = FALSE)

# QUERY -------------------------------------------------------------------
json <- get_customers(api_token, start_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "customers", paste(org, store, sep = "_"), "posabit")
