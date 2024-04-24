box::use(
  hcacova[get_access_token, get_customers],
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

# QUERY -------------------------------------------------------------------
access_token <- get_access_token(client_id, client_secret, username, password)
json <- get_customers(access_token, company_id, start_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "customers", paste(org, store, sep = "_"), "cova")
