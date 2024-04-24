box::use(
  hcacova[get_access_token, get_catalogs],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
company_id <- args$company_id
client_id <- args$client_id
client_secret <- args$client_secret
username <- args$username
password <- args$password

start_utc <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))
end_utc <- today("UTC")

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/cova/", org, "_", store), showWarnings = FALSE)

# QUERY -------------------------------------------------------------------
access_token <- get_access_token(client_id, client_secret, username, password)
json <- get_catalogs(access_token, company_id, start_utc, end_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "catalogs", paste(org, store, sep = "_"), "cova")
