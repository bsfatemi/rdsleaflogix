# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
uuid <- args$uuid
company_id <- args$company_id
client_id <- args$client_id
token <- args$token

start_date <- lubridate::today() - lubridate::days(Sys.getenv("PREVIOUS_DAYS", 1))
end_date <- lubridate::today()

# QUERY -------------------------------------------------------------------
json <- hcagreenbits::get_gb_orders(
  company_id, client_id, token, start = start_date, end = end_date
)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "orders", paste(org, store, sep = "_"), "greenbits")
