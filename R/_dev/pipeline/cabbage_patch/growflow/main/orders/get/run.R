box::use(
  hcagrowflow[get_access_token, get_orders],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
org_name <- args$org_name
client_id <- args$client_id
client_secret <- args$client_secret

start_utc <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))
end_utc <- today("UTC")

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/growflow/", org, "_", store), showWarnings = FALSE)

# QUERY -------------------------------------------------------------------
access_token <- get_access_token(client_id, client_secret)
json <- get_orders(access_token, org_name, start_utc, end_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "orders", paste(org, store, sep = "_"), "growflow")
