box::use(
  hcabudee[get_customers],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
apikey <- args$apikey

start_utc <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))

# QUERY -------------------------------------------------------------------
json <- get_customers(apikey, start_utc)

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/budee/", org, "_", store), showWarnings = FALSE)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "customers", paste(org, store, sep = "_"), "budee")
