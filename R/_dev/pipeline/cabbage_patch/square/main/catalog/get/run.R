box::use(
  hcasquare[get_catalog],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
access_token <- args$access_token

start_utc <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/square/", org, "_", store), showWarnings = FALSE)

# QUERY -------------------------------------------------------------------
json <- get_catalog(access_token, start_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "catalog", paste(org, store, sep = "_"), "square")
